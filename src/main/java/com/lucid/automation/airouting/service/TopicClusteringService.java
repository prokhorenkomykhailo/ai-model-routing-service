package com.lucid.automation.airouting.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucid.automation.airouting.config.TopicClusteringProperties;
import com.lucid.automation.common.dto.topic.TopicClusterDraft;
import com.lucid.automation.common.dto.topic.TopicClusterDraftEvent;
import com.lucid.automation.airouting.model.AITaskType;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.Workspace;
import com.lucid.automation.airouting.producer.TopicDraftProducer;
import com.lucid.automation.airouting.provider.AIProvider;
import com.lucid.automation.airouting.service.AIProviderRouterService;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * Implements Step 1 of the LLM enrichment pipeline: corpus-wide clustering.
 */
@Service
public class TopicClusteringService {

    private static final Logger logger = LoggerFactory.getLogger(TopicClusteringService.class);

    private final TopicClusteringPromptBuilder promptBuilder;
    private final AIProviderRouterService providerRouterService;
    private final TopicDraftProducer topicDraftProducer;
    private final TopicDraftCacheService topicDraftCacheService;
    private final ObjectMapper objectMapper;
    private final TopicClusteringProperties properties;

    public TopicClusteringService(TopicClusteringPromptBuilder promptBuilder,
                                  AIProviderRouterService providerRouterService,
                                  TopicDraftProducer topicDraftProducer,
                                  TopicDraftCacheService topicDraftCacheService,
                                  ObjectMapper objectMapper,
                                  TopicClusteringProperties properties) {
        this.promptBuilder = promptBuilder;
        this.providerRouterService = providerRouterService;
        this.topicDraftProducer = topicDraftProducer;
        this.topicDraftCacheService = topicDraftCacheService;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    /**
    * Process a batch of messages and emit draft clusters.
    */
    public void processBatch(Workspace workspace,
                             List<SlackMessage> messages,
                             Map<String, Object> metadata,
                             int batchNumber) {
        if (workspace == null || CollectionUtils.isEmpty(messages)) {
            logger.warn("Topic clustering skipped: workspace or messages missing");
            return;
        }

        TopicClusteringPromptBuilder.PromptPayload payload = promptBuilder.buildPrompt(messages);
        if (!StringUtils.hasText(payload.prompt())) {
            logger.warn("Topic clustering skipped: empty prompt");
            return;
        }

        AIProvider provider = providerRouterService.selectProvider(
            AITaskType.GENERATE_TOPIC,
            workspace.getTenantId(),
            properties.getProviderHint()
        );
        String providerId = provider != null ? provider.getProviderId() : "unknown";
        if (provider == null) {
            logger.error("No AI provider available for topic clustering");
            return;
        }

        String response = provider.processTextQuery(payload.prompt(), workspace.getDeemergeUserId(), workspace.getTenantId());
        logger.debug("🧾 Raw LLM response ({} chars): {}", response != null ? response.length() : 0,
            response != null ? response.substring(0, Math.min(response.length(), 500)) + (response.length() > 500 ? "..." : "") : "null");
        TopicClusterDraftEvent event = parseResponse(response, payload.messagesUsed(), workspace, providerId, metadata, batchNumber);

        topicDraftProducer.publishDraft(event);
        topicDraftCacheService.cacheDraft(event);
        logger.info("✅ Emitted {} topic drafts for workspace {} batch {}", event.getClusterCount(), workspace.getId(), event.getBatchId());
    }

    private TopicClusterDraftEvent parseResponse(String response,
                                                 List<SlackMessage> messagesUsed,
                                                 Workspace workspace,
                                                 String providerId,
                                                 Map<String, Object> metadata,
                                                 int batchNumber) {
        Map<Integer, String> ordinalToMessageId = buildOrdinalMap(messagesUsed);
        TopicClusterDraftEvent event = new TopicClusterDraftEvent();
        event.setEventId(UUID.randomUUID().toString());
        event.setWorkspaceId(workspace.getId());
        event.setBatchId(workspace.getId() + ":batch_" + batchNumber);
        event.setProviderId(providerId);
        event.setPromptVersion(properties.getPromptVersion());
        event.setCreatedAt(Instant.now());
        Map<String, Object> eventMetadata = new HashMap<>(Optional.ofNullable(metadata).orElseGet(HashMap::new));
        if (workspace.getTenantId() != null) {
            eventMetadata.putIfAbsent("tenantId", workspace.getTenantId());
        }
        if (workspace.getTenantSchema() != null) {
            eventMetadata.putIfAbsent("tenantSchema", workspace.getTenantSchema());
        }
        eventMetadata.putIfAbsent("workspaceId", workspace.getId());
        event.setMetadata(eventMetadata);

        try {
            JsonNode root = tryParseJson(response);
            if (root != null) {
                JsonNode clustersNode = root.get("clusters");
                if (clustersNode != null && clustersNode.isArray()) {
                    for (JsonNode clusterNode : clustersNode) {
                        TopicClusterDraft draft = new TopicClusterDraft();
                        draft.setClusterId(text(clusterNode, "cluster_id", "clusterId"));
                        draft.setDraftTitle(text(clusterNode, "draft_title", "draftTitle"));
                        draft.setChannel(text(clusterNode, "channel"));
                        draft.setThreadId(text(clusterNode, "thread_id", "threadId"));
                        draft.setParticipants(asTextList(clusterNode.get("participants")));
                        draft.setMessageIds(mapMessageIds(clusterNode.get("message_ids"), ordinalToMessageId));
                        event.getClusters().add(draft);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Failed to parse clustering response: {}", e.getMessage(), e);
        }

        // Fallback: if no clusters parsed, create a single bucket so downstream has data to work with.
        if (event.getClusters().isEmpty() && !messagesUsed.isEmpty()) {
            TopicClusterDraft draft = new TopicClusterDraft();
            draft.setClusterId("cluster_auto_1");
            draft.setDraftTitle("Auto clustered batch");
            draft.setParticipants(messagesUsed.stream()
                .map(SlackMessage::getDisplayName)
                .filter(StringUtils::hasText)
                .distinct()
                .toList());
            draft.setChannel(messagesUsed.get(0).getChannelName());
            draft.setThreadId(messagesUsed.get(0).getThreadTs());
            draft.setMessageIds(messagesUsed.stream()
                .map(m -> StringUtils.hasText(m.getId()) ? m.getId() : m.getMessageTs())
                .filter(StringUtils::hasText)
                .toList());
            event.getClusters().add(draft);
            logger.warn("Topic clustering response contained no clusters; emitted fallback cluster with {} messages",
                        draft.getMessageIds().size());
        }

        event.setClusterCount(event.getClusters().size());
        return event;
    }

    private JsonNode tryParseJson(String response) {
        if (!StringUtils.hasText(response)) {
            return null;
        }
        String normalized = normalizeResponse(response);
        try {
            // Use a lenient reader to tolerate trailing commas or small formatting issues
            ObjectMapper lenientMapper = objectMapper.copy()
                .enable(JsonParser.Feature.ALLOW_COMMENTS)
                .enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES)
                .enable(JsonReadFeature.ALLOW_TRAILING_COMMA.mappedFeature())
                .enable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature());
            return lenientMapper.readTree(normalized);
        } catch (Exception e) {
            // Try to extract JSON block from a response that may include prose
            int start = normalized.indexOf('{');
            int end = normalized.lastIndexOf('}');
            if (start >= 0 && end > start) {
                try {
                    String sliced = normalized.substring(start, end + 1);
                    ObjectMapper lenientMapper = objectMapper.copy()
                        .enable(JsonParser.Feature.ALLOW_COMMENTS)
                        .enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES)
                        .enable(JsonReadFeature.ALLOW_TRAILING_COMMA.mappedFeature())
                        .enable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature());
                    return lenientMapper.readTree(sliced);
                } catch (Exception ignored) {
                    logger.debug("Failed to parse sliced JSON block: {}", ignored.getMessage());
                }
            }
            logger.warn("Unable to parse LLM response as JSON; raw response length={} chars", response.length());
            return null;
        }
    }

    private String normalizeResponse(String response) {
        String trimmed = response.trim();
        // Remove common markdown fences anywhere in the string
        trimmed = trimmed.replaceAll("```json", "")
            .replaceAll("```", "")
            .trim();
        // Remove trailing commas before closing braces/brackets
        trimmed = trimmed.replaceAll(",\\s*}", "}")
            .replaceAll(",\\s*]", "]");
        return trimmed;
    }

    private Map<Integer, String> buildOrdinalMap(List<SlackMessage> messagesUsed) {
        Map<Integer, String> map = new HashMap<>();
        int idx = 1;
        for (SlackMessage msg : messagesUsed) {
            map.put(idx++, StringUtils.hasText(msg.getId()) ? msg.getId() : String.valueOf(idx));
        }
        return map;
    }

    private List<String> mapMessageIds(JsonNode node, Map<Integer, String> ordinalToMessageId) {
        if (node == null || !node.isArray()) {
            return List.of();
        }
        return toIntList(node).stream()
            .map(i -> ordinalToMessageId.getOrDefault(i, String.valueOf(i)))
            .collect(Collectors.toList());
    }

    private List<Integer> toIntList(JsonNode node) {
        if (node == null || !node.isArray()) {
            return List.of();
        }
        List<Integer> values = new java.util.ArrayList<>();
        node.forEach(child -> {
            int val = child.asInt(-1);
            if (val > 0) {
                values.add(val);
            }
        });
        return values;
    }

    private List<String> asTextList(JsonNode node) {
        if (node == null) {
            return List.of();
        }
        if (!node.isArray()) {
            if (node.isTextual() && StringUtils.hasText(node.asText())) {
                return List.of(node.asText().trim());
            }
            return List.of();
        }
        List<String> values = new java.util.ArrayList<>();
        for (JsonNode child : node) {
            if (child == null || child.isNull()) {
                continue;
            }
            if (child.isTextual() && StringUtils.hasText(child.asText())) {
                values.add(child.asText().trim());
            } else if (child.isNumber()) {
                values.add(child.asText());
            } else if (child.isObject()) {
                JsonNode name = child.get("name");
                if (name != null && name.isTextual() && StringUtils.hasText(name.asText())) {
                    values.add(name.asText().trim());
                }
            }
        }
        return values.stream().filter(StringUtils::hasText).distinct().toList();
    }

    private String text(JsonNode node, String... keys) {
        if (node == null) {
            return null;
        }
        for (String key : keys) {
            JsonNode val = node.get(key);
            if (val != null && val.isTextual()) {
                return val.asText();
            }
        }
        return null;
    }
}
