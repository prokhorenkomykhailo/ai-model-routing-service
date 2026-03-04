package com.lucid.automation.airouting.service;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucid.automation.airouting.config.TopicActionSuggestionProperties;
import com.lucid.automation.airouting.config.TopicEmbeddingProperties;
import com.lucid.automation.airouting.dto.topic.suggestion.TopicActionSuggestionRequest;
import com.lucid.automation.airouting.dto.topic.suggestion.TopicActionSuggestionResponse;
import com.lucid.automation.airouting.dto.topic.suggestion.TopicActionType;
import com.lucid.automation.airouting.model.AITaskType;
import com.lucid.automation.airouting.provider.AIProvider;
import com.lucid.automation.airouting.util.PromptLoader;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
public class TopicActionSuggestionService {

    private static final Logger logger = LoggerFactory.getLogger(TopicActionSuggestionService.class);

    private final TopicActionSuggestionProperties properties;
    private final TopicEmbeddingProperties embeddingProperties;
    private final AIProviderRouterService providerRouterService;
    private final PromptLoader promptLoader;
    private final ObjectMapper objectMapper;
    private final JdbcTemplate embeddingJdbc;

    public TopicActionSuggestionService(TopicActionSuggestionProperties properties,
                                        TopicEmbeddingProperties embeddingProperties,
                                        AIProviderRouterService providerRouterService,
                                        PromptLoader promptLoader,
                                        ObjectMapper objectMapper,
                                        @Qualifier("topicEmbeddingJdbcTemplate") Optional<JdbcTemplate> topicEmbeddingJdbcTemplate) {
        this.properties = properties;
        this.embeddingProperties = embeddingProperties;
        this.providerRouterService = providerRouterService;
        this.promptLoader = promptLoader;
        this.objectMapper = objectMapper;
        this.embeddingJdbc = topicEmbeddingJdbcTemplate.orElse(null);
    }

    public TopicActionSuggestionResponse suggest(TopicActionSuggestionRequest request) {
        validateRequest(request);
        if (!properties.isEnabled()) {
            throw new IllegalStateException("topic.action-suggestion.enabled=false");
        }

        TopicContext topicContext = loadTopicContext(request.getTopicId(), request.getWorkspaceId());
        String prompt = buildPrompt(request, topicContext);

        AIProvider provider = providerRouterService.selectProvider(
            AITaskType.TEXT_QUERY,
            request.getTenantId(),
            properties.getProviderHint()
        );
        if (provider == null) {
            throw new IllegalStateException("No AI provider available for action suggestion");
        }

        String raw = provider.processTextQuery(prompt, "system", request.getTenantId());
        JsonNode root = tryParseJson(raw);

        String suggestedText = null;
        String suggestedSubject = null;
        if (root != null && root.isObject()) {
            suggestedText = text(root, "suggested_text", "suggestedText", "body", "reply", "text");
            suggestedSubject = text(root, "suggested_subject", "suggestedSubject", "subject");
        }
        if (!StringUtils.hasText(suggestedText)) {
            suggestedText = normalizeRaw(raw, properties.getMaxResponseChars());
        }
        if (!StringUtils.hasText(suggestedText)) {
            suggestedText = defaultSuggestionText(request);
        }
        if (request.getActionType() == TopicActionType.FORWARD && !StringUtils.hasText(suggestedSubject)) {
            suggestedSubject = topicContext.title != null
                ? "Fwd: " + topicContext.title
                : "Fwd: Topic update";
        }

        TopicActionSuggestionResponse response = new TopicActionSuggestionResponse();
        response.setTopicId(request.getTopicId());
        response.setActionType(request.getActionType());
        response.setSuggestedSubject(suggestedSubject);
        response.setSuggestedText(suggestedText);
        response.setProviderId(provider.getProviderId());
        response.setPromptVersion(properties.getPromptVersion());
        response.setTopicContextFound(topicContext.found);
        response.setMetadata(Map.of(
            "promptName", properties.getPromptName(),
            "tone", resolveTone(request),
            "language", resolveLanguage(request),
            "topicContextFound", topicContext.found
        ));
        return response;
    }

    private void validateRequest(TopicActionSuggestionRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("Request is required");
        }
        if (!StringUtils.hasText(request.getTenantId())) {
            throw new IllegalArgumentException("tenantId is required");
        }
        if (request.getActionType() == null) {
            throw new IllegalArgumentException("actionType is required (REPLY/FORWARD)");
        }
    }

    private TopicContext loadTopicContext(String topicId, String workspaceId) {
        if (embeddingJdbc == null || !StringUtils.hasText(topicId)) {
            return new TopicContext(false, null, "{}");
        }
        try {
            String schema = embeddingProperties.getPgvector().getSchema();
            String table = embeddingProperties.getPgvector().getTable();
            String sql;
            List<String> rows;
            if (StringUtils.hasText(workspaceId)) {
                sql = "SELECT topic_metadata::text FROM " + schema + "." + table
                    + " WHERE topic_id::text = ? AND workspace_id = ? ORDER BY updated_at DESC LIMIT 1";
                rows = embeddingJdbc.query(sql, (rs, i) -> rs.getString(1), topicId, workspaceId);
            } else {
                sql = "SELECT topic_metadata::text FROM " + schema + "." + table
                    + " WHERE topic_id::text = ? ORDER BY updated_at DESC LIMIT 1";
                rows = embeddingJdbc.query(sql, (rs, i) -> rs.getString(1), topicId);
            }
            if (rows.isEmpty() || !StringUtils.hasText(rows.get(0))) {
                return new TopicContext(false, null, "{}");
            }
            JsonNode node = tryParseJson(rows.get(0));
            String title = node != null ? text(node, "title") : null;
            String topicJson = normalizeRaw(rows.get(0), properties.getMaxContextChars());
            return new TopicContext(true, title, topicJson);
        } catch (Exception e) {
            logger.warn("Failed loading topic context for topicId={}: {}", topicId, e.getMessage());
            return new TopicContext(false, null, "{}");
        }
    }

    private String buildPrompt(TopicActionSuggestionRequest request, TopicContext topicContext) {
        String template = promptLoader.loadPromptTemplate(properties.getPromptName());
        Map<String, String> values = new LinkedHashMap<>();
        values.put("{{action_type}}", request.getActionType().name().toLowerCase());
        values.put("{{user_name}}", safe(request.getUserDisplayName()));
        values.put("{{recipient}}", safe(request.getRecipient()));
        values.put("{{tone}}", resolveTone(request));
        values.put("{{language}}", resolveLanguage(request));
        values.put("{{additional_instruction}}", safe(request.getAdditionalInstruction()));
        values.put("{{latest_message}}", safe(request.getLatestMessage()));
        values.put("{{topic_id}}", safe(request.getTopicId()));
        values.put("{{topic_context_json}}", topicContext.topicJson);

        String prompt = template;
        for (Map.Entry<String, String> e : values.entrySet()) {
            prompt = prompt.replace(e.getKey(), e.getValue());
        }
        return normalizeRaw(prompt, properties.getMaxContextChars());
    }

    private String resolveTone(TopicActionSuggestionRequest request) {
        return StringUtils.hasText(request.getTone())
            ? request.getTone().trim()
            : properties.getDefaultTone();
    }

    private String resolveLanguage(TopicActionSuggestionRequest request) {
        return StringUtils.hasText(request.getLanguage())
            ? request.getLanguage().trim()
            : properties.getDefaultLanguage();
    }

    private String safe(String value) {
        return StringUtils.hasText(value) ? value.trim() : "";
    }

    private String defaultSuggestionText(TopicActionSuggestionRequest request) {
        String recipient = StringUtils.hasText(request.getRecipient()) ? request.getRecipient().trim() : "team";
        if (request.getActionType() == TopicActionType.FORWARD) {
            return "Forwarding this thread to " + recipient + " for review and next steps.";
        }
        return "Hi " + recipient + ", sharing a quick update from the thread. Please confirm next steps.";
    }

    private String normalizeRaw(String value, int maxChars) {
        if (!StringUtils.hasText(value)) {
            return "";
        }
        String normalized = value.trim().replace("```json", "").replace("```", "").trim();
        return normalized.length() <= maxChars ? normalized : normalized.substring(0, maxChars);
    }

    private JsonNode tryParseJson(String raw) {
        if (!StringUtils.hasText(raw)) {
            return null;
        }
        String normalized = normalizeRaw(raw, Math.max(properties.getMaxResponseChars(), properties.getMaxContextChars()))
            .replaceAll(",\\s*}", "}")
            .replaceAll(",\\s*]", "]");
        try {
            ObjectMapper lenient = objectMapper.copy()
                .enable(JsonParser.Feature.ALLOW_COMMENTS)
                .enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES)
                .enable(JsonReadFeature.ALLOW_TRAILING_COMMA.mappedFeature())
                .enable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature());
            return lenient.readTree(normalized);
        } catch (Exception e) {
            int start = normalized.indexOf('{');
            int end = normalized.lastIndexOf('}');
            if (start >= 0 && end > start) {
                try {
                    return objectMapper.readTree(normalized.substring(start, end + 1));
                } catch (Exception ignored) {
                    return null;
                }
            }
            return null;
        }
    }

    private String text(JsonNode node, String... keys) {
        if (node == null || keys == null) {
            return null;
        }
        for (String key : keys) {
            JsonNode value = node.get(key);
            if (value != null && value.isTextual() && StringUtils.hasText(value.asText())) {
                return value.asText().trim();
            }
        }
        return null;
    }

    private record TopicContext(boolean found, String title, String topicJson) {}
}
