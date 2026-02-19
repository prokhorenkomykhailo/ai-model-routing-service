package com.lucid.automation.airouting.service;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucid.automation.airouting.config.TopicEmbeddingProperties;
import com.lucid.automation.airouting.config.TopicUpdateProperties;
import com.lucid.automation.airouting.dto.topic.TopicActionItem;
import com.lucid.automation.airouting.dto.topic.TopicMetadata;
import com.lucid.automation.airouting.dto.topic.TopicMetadataEvent;
import com.lucid.automation.airouting.model.AITaskType;
import com.lucid.automation.airouting.provider.AIProvider;
import com.lucid.automation.airouting.producer.TopicMetadataProducer;
import com.lucid.automation.airouting.util.PromptLoader;
import com.lucid.automation.common.dto.messaging.IngestionEventDTO;
import com.lucid.automation.common.dto.messaging.IngestionMessageDTO;
import com.lucid.automation.common.dto.messaging.IngestionUserDTO;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
public class TopicUpdateManager {

    private static final Logger logger = LoggerFactory.getLogger(TopicUpdateManager.class);

    private final TopicUpdateProperties properties;
    private final TopicEmbeddingStoreService embeddingStoreService;
    private final TopicMetadataProducer metadataProducer;
    private final AIProviderRouterService providerRouterService;
    private final PromptLoader promptLoader;
    private final ObjectMapper objectMapper;
    private final JdbcTemplate embeddingJdbc;
    private final TopicEmbeddingProperties embeddingProperties;

    record BelongsCheckDecision(
        boolean performed,
        boolean belongs,
        double confidence,
        String explanation,
        String providerId
    ) {}

    record SimilaritySearchContext(
        boolean executed,
        int topK,
        double threshold,
        List<TopicEmbeddingStoreService.ScoredTopic> candidates,
        TopicEmbeddingStoreService.ScoredTopic selected,
        String error,
        List<Map<String, Object>> evaluations
    ) {}

    public TopicUpdateManager(TopicUpdateProperties properties,
                             TopicEmbeddingStoreService embeddingStoreService,
                             TopicMetadataProducer metadataProducer,
                             AIProviderRouterService providerRouterService,
                             PromptLoader promptLoader,
                             ObjectMapper objectMapper,
                             @Qualifier("topicEmbeddingJdbcTemplate") Optional<JdbcTemplate> topicEmbeddingJdbcTemplate,
                             TopicEmbeddingProperties embeddingProperties) {
        this.properties = properties;
        this.embeddingStoreService = embeddingStoreService;
        this.metadataProducer = metadataProducer;
        this.providerRouterService = providerRouterService;
        this.promptLoader = promptLoader;
        this.objectMapper = objectMapper;
        this.embeddingJdbc = topicEmbeddingJdbcTemplate.orElse(null);
        this.embeddingProperties = embeddingProperties;
    }

    public void handleNewMessage(IngestionEventDTO event) {
        if (event == null || event.getMessage() == null) {
            return;
        }

        String tenantId = event.getTenantId();
        IngestionMessageDTO msg = event.getMessage();
        IngestionUserDTO user = event.getUser();

        String workspaceId = msg.getBestWorkspaceId();
        String channelName = msg.getChannelName();
        String channelId = msg.getChannelId();
        String threadTs = msg.getThreadTs();
        String messageTs = msg.getTs();
        String text = msg.getText();

        if (!StringUtils.hasText(tenantId) || !StringUtils.hasText(workspaceId) || !StringUtils.hasText(text)) {
            return;
        }

        String messageId = buildMessageId(tenantId, workspaceId, channelId, threadTs, messageTs);
        String authorLabel = userLabel(user);

        // Fast-path: if we already created a topic for this Slack thread in this workspace/channel, update it directly.
        // This keeps Step 6 behavior sensible even when vector embeddings are unavailable or low-quality.
        if (StringUtils.hasText(threadTs)) {
            String byThread = findTopicIdByThread(workspaceId, channelName, threadTs);
            if (StringUtils.hasText(byThread)) {
                TopicMetadata existing = fetchTopicMetadata(byThread);
                if (existing != null) {
                    SimilaritySearchContext searchCtx = new SimilaritySearchContext(false, properties.getSearchTopK(),
                        properties.getSimilarityThreshold(), List.of(), null, null, List.of());
                    publishUpdatedTopic(tenantId, workspaceId, byThread, existing, messageId, channelName, channelId, threadTs,
                        authorLabel, text, null, "same_thread",
                        new BelongsCheckDecision(false, true, 1.0, "same_thread", null),
                        searchCtx,
                        "thread_fast_path");
                    return;
                }
            }
        }

        TopicMetadata query = new TopicMetadata();
        query.setChannel(channelName);
        query.setSummary(text);
        String derivedTitle = deriveTitle(text);
        query.setTitle(derivedTitle);
        if (StringUtils.hasText(authorLabel)) {
            query.setParticipants(List.of(authorLabel));
        }
        query.setTags(deriveTags(query.getExternalParty(), channelName, derivedTitle));

        List<TopicEmbeddingStoreService.ScoredTopic> candidates;
        SimilaritySearchContext searchCtx;
        try {
            candidates = embeddingStoreService.search(workspaceId, query, properties.getSearchTopK());
            TopicEmbeddingStoreService.ScoredTopic selected = candidates.isEmpty() ? null : candidates.get(0);
            searchCtx = new SimilaritySearchContext(true, properties.getSearchTopK(),
                properties.getSimilarityThreshold(), candidates, selected, null,
                candidateEvaluations(workspaceId, channelName, candidates, selected, null, "retrieved_candidates"));
        } catch (Exception e) {
            candidates = List.of();
            searchCtx = new SimilaritySearchContext(true, properties.getSearchTopK(),
                properties.getSimilarityThreshold(), List.of(), null, safe(e.getMessage()), List.of());
        }

        TopicEmbeddingStoreService.ScoredTopic best = candidates.isEmpty() ? null : candidates.get(0);
        if (best == null) {
            publishNewTopic(tenantId, workspaceId, channelName, messageId, channelId, threadTs, authorLabel, text,
                searchCtx.error() != null ? "vector_search_error" : "no_candidate",
                null,
                new BelongsCheckDecision(false, false, 0.0, searchCtx.error() != null ? "vector_search_error" : "no_candidate", null),
                searchCtx,
                searchCtx.error() != null ? "semantic_similarity_path_error" : "semantic_similarity_path_no_candidate");
            return;
        }

        if (best.score() < properties.getSimilarityThreshold()) {
            SimilaritySearchContext ctx = new SimilaritySearchContext(searchCtx.executed(), searchCtx.topK(), searchCtx.threshold(),
                searchCtx.candidates(), searchCtx.selected(), searchCtx.error(),
                candidateEvaluations(workspaceId, channelName, searchCtx.candidates(), best, null, "rejected_below_threshold"));
            publishNewTopic(tenantId, workspaceId, channelName, messageId, channelId, threadTs, authorLabel, text,
                "below_similarity_threshold", best.score(),
                new BelongsCheckDecision(false, false, 0.0, "below_similarity_threshold", null),
                ctx,
                "semantic_similarity_path_below_threshold");
            return;
        }

        TopicMetadata existing = fetchTopicMetadata(best.topicId());
        if (existing == null) {
            SimilaritySearchContext ctx = new SimilaritySearchContext(searchCtx.executed(), searchCtx.topK(), searchCtx.threshold(),
                searchCtx.candidates(), searchCtx.selected(), searchCtx.error(),
                candidateEvaluations(workspaceId, channelName, searchCtx.candidates(), best, null, "rejected_missing_metadata"));
            publishNewTopic(tenantId, workspaceId, channelName, messageId, channelId, threadTs, authorLabel, text,
                "missing_existing_topic_metadata", best.score(),
                new BelongsCheckDecision(false, false, 0.0, "missing_existing_topic_metadata", null),
                ctx,
                "semantic_similarity_path_missing_metadata");
            return;
        }

        if (properties.isRequireSameChannel() && StringUtils.hasText(channelName) && StringUtils.hasText(existing.getChannel())
            && !existing.getChannel().equals(channelName)) {
            SimilaritySearchContext ctx = new SimilaritySearchContext(searchCtx.executed(), searchCtx.topK(), searchCtx.threshold(),
                searchCtx.candidates(), searchCtx.selected(), searchCtx.error(),
                candidateEvaluations(workspaceId, channelName, searchCtx.candidates(), best, null, "rejected_channel_mismatch"));
            publishNewTopic(tenantId, workspaceId, channelName, messageId, channelId, threadTs, authorLabel, text,
                "channel_mismatch", best.score(), new BelongsCheckDecision(false, false, 0.0, "channel_mismatch", null),
                ctx,
                "semantic_similarity_path_channel_mismatch");
            return;
        }

        BelongsCheckDecision decision = properties.isBelongsCheckEnabled()
            ? belongsToTopic(tenantId, existing, channelName, authorLabel, text)
            : new BelongsCheckDecision(false, true, 1.0, "belongs_check_disabled", null);

        if (properties.isBelongsCheckEnabled() && !decision.belongs()) {
            SimilaritySearchContext ctx = new SimilaritySearchContext(searchCtx.executed(), searchCtx.topK(), searchCtx.threshold(),
                searchCtx.candidates(), searchCtx.selected(), searchCtx.error(),
                candidateEvaluations(workspaceId, channelName, searchCtx.candidates(), best, null, "rejected_belongs_check_blocked"));
            publishNewTopic(tenantId, workspaceId, channelName, messageId, channelId, threadTs, authorLabel, text,
                "belongs_check_negative", best.score(), decision, ctx,
                "semantic_similarity_path_belongs_blocked");
            return;
        }

        String updateReason = properties.isBelongsCheckEnabled() ? "belongs_check_positive" : "similarity_above_threshold";
        SimilaritySearchContext ctx = new SimilaritySearchContext(searchCtx.executed(), searchCtx.topK(), searchCtx.threshold(),
            searchCtx.candidates(), searchCtx.selected(), searchCtx.error(),
            candidateEvaluations(workspaceId, channelName, searchCtx.candidates(), best, best, "accepted_selected_best_score"));
        publishUpdatedTopic(tenantId, workspaceId, best.topicId(), existing, messageId, channelName, channelId, threadTs,
            authorLabel, text, best.score(), updateReason, decision, ctx,
            "semantic_similarity_path_update");
    }

    private String deriveTitle(String text) {
        if (!StringUtils.hasText(text)) {
            return "New topic";
        }
        String normalized = text.trim().replaceAll("\\s+", " ");
        if (normalized.length() > 80) {
            normalized = normalized.substring(0, 80).trim();
        }
        return normalized;
    }

    private List<String> deriveTags(String externalParty, String channel, String title) {
        List<String> tags = new ArrayList<>();
        if (StringUtils.hasText(externalParty)) {
            tags.add(externalParty.trim());
        }
        if (StringUtils.hasText(channel)) {
            String c = channel.startsWith("#") ? channel.substring(1) : channel;
            if (StringUtils.hasText(c)) {
                tags.add(c);
            }
        }
        if (StringUtils.hasText(title)) {
            String[] parts = title.toLowerCase().replaceAll("[^a-z0-9 ]", " ").split("\\s+");
            for (String p : parts) {
                if (p.length() >= 3 && tags.size() < 8) {
                    tags.add(p);
                }
            }
        }
        return tags.isEmpty() ? List.of("topic") : tags.stream().distinct().toList();
    }

    private void publishUpdatedTopic(String tenantId,
                                     String workspaceId,
                                     String topicId,
                                     TopicMetadata existing,
                                     String messageId,
                                     String channelName,
                                     String channelId,
                                     String threadTs,
                                     String author,
                                     String text,
                                     Double similarityScore,
                                     String reason,
                                     BelongsCheckDecision decision,
                                     SimilaritySearchContext searchContext,
                                     String decisionPath) {
        AIProvider provider = null;
        TopicMetadata updated = null;
        boolean llmGenerated = false;
        try {
            provider = providerRouterService.selectProvider(AITaskType.GENERATE_TOPIC, tenantId, properties.getBelongsCheckModelHint());
            String prompt = buildUpdatePrompt(existing, author, text, channelName);
            updated = parseTopicMetadata(provider.processTextQuery(prompt, "system", tenantId));
            llmGenerated = updated != null;
        } catch (Exception e) {
            logger.warn("Step6 update: provider unavailable, falling back to deterministic metadata: {}", e.getMessage());
        }
        TopicMetadata finalTopic = ensureMinimumFields(updated, existing, channelName, author, text);

        int previousVersion = fetchTopicVersion(topicId);
        int nextVersion = previousVersion > 0 ? previousVersion + 1 : 1;

        TopicMetadataEvent out = new TopicMetadataEvent();
        out.setEventId(UUID.randomUUID().toString());
        out.setCreatedAt(Instant.now());
        out.setWorkspaceId(workspaceId);
        out.setBatchId("step6:" + workspaceId + ":" + safe(messageId));
        out.setClusterId("topic:" + topicId);
        out.setTopicId(topicId);
        out.setProviderId(provider != null ? provider.getProviderId() : "fallback");
        out.setPromptVersion(properties.getPromptVersion());
        out.setMessageIds(List.of(messageId));
        out.setTopic(finalTopic);
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("step", "6");
        meta.put("action", "update");
        meta.put("reason", reason);
        meta.put("decisionPath", decisionPath);
        meta.put("decisionPathCategory", decisionPathCategory(decisionPath));
        meta.put("topicVersion", nextVersion);
        if (previousVersion > 0) {
            meta.put("previousTopicVersion", previousVersion);
        }
        meta.put("searchTopK", properties.getSearchTopK());
        meta.put("similarityThreshold", properties.getSimilarityThreshold());
        meta.put("appliedSimilarityThreshold", properties.getSimilarityThreshold());
        if (similarityScore != null) {
            meta.put("similarityScore", similarityScore);
        }
        if (StringUtils.hasText(threadTs)) {
            meta.put("threadTs", threadTs.trim());
        }
        if (StringUtils.hasText(channelId)) {
            meta.put("channelId", channelId.trim());
        }
        meta.put("belongsCheck", belongsCheckMetadata(decision));
        meta.put("similaritySearch", similaritySearchMetadata(searchContext));
        meta.put("metadataFullyRegenerated", true);
        meta.put("metadataRegeneratedOnUpdate", llmGenerated);
        meta.put("metadataGenerationMode", llmGenerated ? "llm" : "fallback");
        meta.put("embeddingsRecalculatedAfterUpdate", true);
        meta.put("embeddingPersistedBy", "step4_pgvector_upsert");
        meta.put("embeddingUpdate", Map.of(
            "scheduled", true,
            "store", "pgvector",
            "via", "step4_pgvector_upsert",
            "status", "async"
        ));
        out.setMetadata(meta);

        try {
            metadataProducer.publish(out);
            logger.info("Step6 updated topicId={} workspaceId={} reason={} score={}", topicId, workspaceId, reason, similarityScore);
        } catch (Exception e) {
            metadataProducer.publishDlq(safeJson(out), "step6_publish_error:" + safe(e.getMessage()), workspaceId, out.getBatchId());
            logger.warn("Step6 publish failed (update) topicId={} workspaceId={} err={}", topicId, workspaceId, e.getMessage());
        }
    }

    private void publishNewTopic(String tenantId,
                                 String workspaceId,
                                 String channelName,
                                 String messageId,
                                 String channelId,
                                 String threadTs,
                                 String author,
                                 String text,
                                 String reason,
                                 Double similarityScore,
                                 BelongsCheckDecision decision,
                                 SimilaritySearchContext searchContext,
                                 String decisionPath) {
        TopicMetadata seed = new TopicMetadata();
        seed.setChannel(channelName);
        seed.setSummary(text);
        AIProvider provider = null;
        TopicMetadata created = null;
        boolean llmGenerated = false;
        try {
            provider = providerRouterService.selectProvider(AITaskType.GENERATE_TOPIC, tenantId, properties.getBelongsCheckModelHint());
            String prompt = buildUpdatePrompt(seed, author, text, channelName);
            created = parseTopicMetadata(provider.processTextQuery(prompt, "system", tenantId));
            llmGenerated = created != null;
        } catch (Exception e) {
            logger.warn("Step6 create: provider unavailable, falling back to deterministic metadata: {}", e.getMessage());
        }
        TopicMetadata finalTopic = ensureMinimumFields(created, seed, channelName, author, text);

        String topicId = stableTopicId(workspaceId, channelName, messageId);

        int previousVersion = fetchTopicVersion(topicId);
        int nextVersion = previousVersion > 0 ? previousVersion + 1 : 1;

        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("step", "6");
        meta.put("action", "create");
        meta.put("reason", reason);
        meta.put("decisionPath", decisionPath);
        meta.put("decisionPathCategory", decisionPathCategory(decisionPath));
        meta.put("topicVersion", nextVersion);
        if (previousVersion > 0) {
            meta.put("previousTopicVersion", previousVersion);
        }
        meta.put("searchTopK", properties.getSearchTopK());
        meta.put("similarityThreshold", properties.getSimilarityThreshold());
        meta.put("appliedSimilarityThreshold", properties.getSimilarityThreshold());
        if (similarityScore != null) {
            meta.put("similarityScore", similarityScore);
        }
        if (StringUtils.hasText(threadTs)) {
            meta.put("threadTs", threadTs.trim());
        }
        if (StringUtils.hasText(channelId)) {
            meta.put("channelId", channelId.trim());
        }
        meta.put("belongsCheck", belongsCheckMetadata(decision));
        meta.put("similaritySearch", similaritySearchMetadata(searchContext));
        meta.put("metadataFullyRegenerated", true);
        meta.put("metadataRegeneratedOnUpdate", llmGenerated);
        meta.put("metadataGenerationMode", llmGenerated ? "llm" : "fallback");
        meta.put("embeddingsRecalculatedAfterUpdate", true);
        meta.put("embeddingPersistedBy", "step4_pgvector_upsert");
        meta.put("embeddingUpdate", Map.of(
            "scheduled", true,
            "store", "pgvector",
            "via", "step4_pgvector_upsert",
            "status", "async"
        ));

        TopicMetadataEvent out = new TopicMetadataEvent();
        out.setEventId(UUID.randomUUID().toString());
        out.setCreatedAt(Instant.now());
        out.setWorkspaceId(workspaceId);
        out.setBatchId("step6:" + workspaceId + ":" + safe(messageId));
        out.setClusterId("topic:" + topicId);
        out.setTopicId(topicId);
        out.setProviderId(provider != null ? provider.getProviderId() : "fallback");
        out.setPromptVersion(properties.getPromptVersion());
        out.setMessageIds(List.of(messageId));
        out.setTopic(finalTopic);
        out.setMetadata(meta);

        try {
            metadataProducer.publish(out);
            logger.info("Step6 created topicId={} workspaceId={} reason={}", topicId, workspaceId, reason);
        } catch (Exception e) {
            metadataProducer.publishDlq(safeJson(out), "step6_publish_error:" + safe(e.getMessage()), workspaceId, out.getBatchId());
            logger.warn("Step6 publish failed (create) topicId={} workspaceId={} err={}", topicId, workspaceId, e.getMessage());
        }
    }

    private String findTopicIdByThread(String workspaceId, String channelName, String threadTs) {
        if (embeddingJdbc == null || !StringUtils.hasText(workspaceId) || !StringUtils.hasText(channelName) || !StringUtils.hasText(threadTs)) {
            return null;
        }
        try {
            String schema = embeddingProperties.getPgvector().getSchema();
            String table = embeddingProperties.getPgvector().getTable();
            String sql = """
                SELECT topic_id::text AS topic_id
                  FROM %s.%s
                 WHERE workspace_id = ?
                   AND topic_metadata->>'channel' = ?
                   AND topic_metadata->>'threadTs' = ?
                 ORDER BY updated_at DESC
                 LIMIT 1
                """.formatted(schema, table);
            List<String> rows = embeddingJdbc.query(sql, (rs, i) -> rs.getString("topic_id"),
                workspaceId, channelName, threadTs.trim());
            return rows.isEmpty() ? null : rows.get(0);
        } catch (Exception e) {
            return null;
        }
    }

    private BelongsCheckDecision belongsToTopic(String tenantId, TopicMetadata existing, String channelName, String author, String text) {
        String existingJson = safeJson(existing);
        String prompt = """
Return STRICT JSON ONLY like: { "belongs": true|false, "confidence": 0.0-1.0, "explanation": "short reason" }.

Decide if the new message should be appended to the existing topic.
Consider channel context, participants, and whether it continues the same workstream.

Existing topic JSON:
%s

New message (channel=%s, author=%s):
%s
""".formatted(existingJson, safe(channelName), safe(author), safe(text));

        JsonNode root;
        AIProvider provider = null;
        try {
            provider = providerRouterService.selectProvider(AITaskType.TEXT_QUERY, tenantId, properties.getBelongsCheckModelHint());
            String response = provider.processTextQuery(prompt, "system", tenantId);
            root = tryParseJson(response);
        } catch (Exception e) {
            logger.warn("Step6 belongs-check: provider unavailable, defaulting to belongs=true: {}", e.getMessage());
            return new BelongsCheckDecision(false, true, 1.0, "provider_unavailable_default_true", provider != null ? provider.getProviderId() : null);
        }
        if (root == null) {
            return new BelongsCheckDecision(false, true, 1.0, "unparseable_response_default_true", provider != null ? provider.getProviderId() : null);
        }

        boolean belongs = true;
        JsonNode belongsNode = root.get("belongs");
        if (belongsNode != null && belongsNode.isBoolean()) {
            belongs = belongsNode.asBoolean();
        }

        double confidence = 0.5;
        JsonNode confNode = root.get("confidence");
        if (confNode != null && confNode.isNumber()) {
            confidence = Math.max(0.0, Math.min(1.0, confNode.asDouble()));
        }

        String explanation = null;
        JsonNode expNode = root.get("explanation");
        if (expNode != null && expNode.isTextual()) {
            explanation = expNode.asText();
        }
        if (!StringUtils.hasText(explanation)) {
            explanation = belongs ? "belongs_true" : "belongs_false";
        }

        return new BelongsCheckDecision(true, belongs, confidence, explanation, provider != null ? provider.getProviderId() : null);
    }

    private Map<String, Object> belongsCheckMetadata(BelongsCheckDecision decision) {
        if (decision == null) {
            return Map.of(
                "performed", false,
                "belongs", true,
                "confidence", 1.0,
                "explanation", "missing_decision_default_true"
            );
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("performed", decision.performed());
        m.put("belongs", decision.belongs());
        m.put("confidence", decision.confidence());
        m.put("explanation", decision.explanation());
        if (StringUtils.hasText(decision.providerId())) {
            m.put("providerId", decision.providerId());
        }
        m.put("modelHint", properties.getBelongsCheckModelHint());
        return m;
    }

    private Map<String, Object> similaritySearchMetadata(SimilaritySearchContext ctx) {
        if (ctx == null) {
            return Map.of("executed", false);
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("executed", ctx.executed());
        m.put("topK", ctx.topK());
        m.put("threshold", ctx.threshold());
        m.put("requireSameChannel", properties.isRequireSameChannel());
        if (StringUtils.hasText(ctx.error())) {
            m.put("error", ctx.error());
        }

        m.put("retrievedCandidates", rawCandidates(ctx.candidates(), ctx.topK()));
        m.put("candidates", ctx.evaluations() != null ? ctx.evaluations() : List.of());
        Map<String, Object> selected = new LinkedHashMap<>();
        selected.put("topicId", ctx.selected() != null ? ctx.selected().topicId() : null);
        selected.put("score", ctx.selected() != null ? ctx.selected().score() : null);
        m.put("selected", selected);
        return m;
    }

    private List<Map<String, Object>> candidateEvaluations(String workspaceId,
                                                           String channelName,
                                                           List<TopicEmbeddingStoreService.ScoredTopic> candidates,
                                                           TopicEmbeddingStoreService.ScoredTopic selected,
                                                           TopicEmbeddingStoreService.ScoredTopic accepted,
                                                           String decisionOutcome) {
        if (candidates == null || candidates.isEmpty()) {
            return List.of();
        }
        List<Map<String, Object>> out = new ArrayList<>();
        for (TopicEmbeddingStoreService.ScoredTopic c : candidates) {
            if (c == null) continue;
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("topicId", c.topicId());
            m.put("score", c.score());
            boolean isSelected = selected != null && selected.topicId().equals(c.topicId());
            boolean isAccepted = accepted != null && accepted.topicId().equals(c.topicId());
            m.put("selected", isSelected);
            m.put("accepted", isAccepted);
            if (isAccepted) {
                m.put("acceptedReason", decisionOutcome);
            } else {
                m.put("rejectedReason", computeRejectedReason(workspaceId, channelName, c, isSelected, decisionOutcome));
            }
            out.add(m);
            if (out.size() >= Math.max(1, properties.getSearchTopK())) {
                break;
            }
        }
        return out;
    }

    private List<Map<String, Object>> rawCandidates(List<TopicEmbeddingStoreService.ScoredTopic> candidates, int cap) {
        if (candidates == null || candidates.isEmpty()) {
            return List.of();
        }
        List<Map<String, Object>> out = new ArrayList<>();
        for (TopicEmbeddingStoreService.ScoredTopic c : candidates) {
            if (c == null) continue;
            out.add(Map.of("topicId", c.topicId(), "score", c.score()));
            if (out.size() >= Math.max(1, cap)) {
                break;
            }
        }
        return out;
    }

    private String computeRejectedReason(String workspaceId,
                                         String channelName,
                                         TopicEmbeddingStoreService.ScoredTopic candidate,
                                         boolean isSelected,
                                         String decisionOutcome) {
        if (candidate == null) {
            return "rejected_null_candidate";
        }
        if (candidate.score() < properties.getSimilarityThreshold()) {
            return "rejected_below_threshold";
        }
        if (isSelected) {
            if ("rejected_belongs_check_blocked".equals(decisionOutcome)) {
                return "rejected_belongs_check_blocked";
            }
            if ("rejected_missing_metadata".equals(decisionOutcome)) {
                return "rejected_missing_metadata";
            }
            if ("rejected_channel_mismatch".equals(decisionOutcome)) {
                return "rejected_channel_mismatch";
            }
            if ("rejected_below_threshold".equals(decisionOutcome)) {
                return "rejected_below_threshold";
            }
        }

        TopicMetadata meta = fetchTopicMetadata(candidate.topicId());
        if (meta == null) {
            return "rejected_missing_metadata";
        }
        if (properties.isRequireSameChannel()
            && StringUtils.hasText(channelName)
            && StringUtils.hasText(meta.getChannel())
            && !channelName.equals(meta.getChannel())) {
            return "rejected_channel_mismatch";
        }
        return "not_selected_lower_score";
    }

    private String decisionPathCategory(String decisionPath) {
        if (!StringUtils.hasText(decisionPath)) {
            return "unknown";
        }
        String normalized = decisionPath.trim().toLowerCase();
        if (normalized.contains("thread_fast_path") || normalized.contains("same_thread") || normalized.startsWith("thread")) {
            return "thread_fast_path";
        }
        if (normalized.contains("belongs_blocked")) {
            return "semantic_similarity_belongs_blocked";
        }
        if (normalized.contains("semantic_similarity")) {
            return "semantic_similarity_path";
        }
        return "unknown";
    }

    private int fetchTopicVersion(String topicId) {
        if (embeddingJdbc == null || !StringUtils.hasText(topicId)) {
            return 0;
        }
        try {
            String schema = embeddingProperties.getPgvector().getSchema();
            String table = embeddingProperties.getPgvector().getTable();
            String sql = "SELECT topic_metadata->>'version' AS v FROM " + schema + "." + table + " WHERE topic_id::text = ?";
            List<String> rows = embeddingJdbc.query(sql, (rs, i) -> rs.getString("v"), topicId);
            if (rows.isEmpty() || !StringUtils.hasText(rows.get(0))) {
                return 0;
            }
            return Integer.parseInt(rows.get(0).trim());
        } catch (Exception e) {
            return 0;
        }
    }

    private String buildUpdatePrompt(TopicMetadata existing, String author, String text, String channelName) {
        String template = promptLoader.loadPromptTemplate(properties.getUpdatePromptName());
        String existingJson = safeJson(existing);
        String msg = String.format("Author: %s | Channel: %s | Text: %s", safe(author), safe(channelName), safe(text));
        return template
            .replace("{{existing_topic_json}}", existingJson)
            .replace("{{new_messages}}", msg);
    }

    private TopicMetadata fetchTopicMetadata(String topicId) {
        if (embeddingJdbc == null || !StringUtils.hasText(topicId)) {
            return null;
        }
        try {
            String schema = embeddingProperties.getPgvector().getSchema();
            String table = embeddingProperties.getPgvector().getTable();
            String sql = "SELECT topic_metadata::text AS meta FROM " + schema + "." + table + " WHERE topic_id::text = ?";
            List<String> rows = embeddingJdbc.query(sql, (rs, i) -> rs.getString("meta"), topicId);
            if (rows.isEmpty() || !StringUtils.hasText(rows.get(0))) {
                return null;
            }
            JsonNode node = objectMapper.readTree(rows.get(0));
            return parseTopicMetadataNode(node);
        } catch (Exception e) {
            return null;
        }
    }

    private TopicMetadata ensureMinimumFields(TopicMetadata candidate,
                                             TopicMetadata fallback,
                                             String channelName,
                                             String author,
                                             String text) {
        TopicMetadata topic = candidate != null ? candidate : fallback != null ? fallback : new TopicMetadata();

        if (!StringUtils.hasText(topic.getChannel())) {
            topic.setChannel(channelName);
        }

        if (!StringUtils.hasText(topic.getTitle())) {
            String base = StringUtils.hasText(text) ? text.trim() : "New topic";
            String normalized = base.replaceAll("\\s+", " ");
            if (normalized.length() > 80) {
                normalized = normalized.substring(0, 80).trim();
            }
            topic.setTitle(normalized);
        }

        if (!StringUtils.hasText(topic.getSummary())) {
            topic.setSummary(StringUtils.hasText(text) ? text.trim() : "");
        }

        if (topic.getParticipants() == null || topic.getParticipants().isEmpty()) {
            Set<String> participants = new LinkedHashSet<>();
            if (StringUtils.hasText(author)) {
                participants.add(author.trim());
            }
            topic.setParticipants(new ArrayList<>(participants));
        }

        if (topic.getTags() == null || topic.getTags().isEmpty()) {
            List<String> tags = new ArrayList<>();
            if (StringUtils.hasText(topic.getExternalParty())) {
                tags.add(topic.getExternalParty());
            }
            if (StringUtils.hasText(topic.getChannel())) {
                String c = topic.getChannel().startsWith("#") ? topic.getChannel().substring(1) : topic.getChannel();
                if (StringUtils.hasText(c)) {
                    tags.add(c);
                }
            }
            if (StringUtils.hasText(topic.getTitle())) {
                String[] parts = topic.getTitle().toLowerCase().replaceAll("[^a-z0-9 ]", " ").split("\\s+");
                for (String p : parts) {
                    if (p.length() >= 3 && tags.size() < 8) tags.add(p);
                }
            }
            topic.setTags(tags.isEmpty() ? List.of("topic") : tags.stream().distinct().toList());
        }

        if (topic.getActionItems() == null || topic.getActionItems().isEmpty()) {
            TopicActionItem item = new TopicActionItem();
            item.setTask(deriveTask(topic, text));
            item.setOwner(!topic.getParticipants().isEmpty() ? topic.getParticipants().get(0) : author);
            item.setStatus("pending");
            item.setPriority(StringUtils.hasText(topic.getUrgency()) ? topic.getUrgency() : "medium");
            topic.setActionItems(List.of(item));
        }

        return topic;
    }

    private String deriveTask(TopicMetadata topic, String text) {
        if (topic != null && StringUtils.hasText(topic.getSummary())) {
            String s = topic.getSummary().trim();
            int cut = s.indexOf('.');
            return cut > 0 ? s.substring(0, cut + 1).trim() : s;
        }
        if (StringUtils.hasText(text)) {
            String t = text.trim();
            return t.length() > 180 ? t.substring(0, 180).trim() : t;
        }
        return "Follow up";
    }

    private TopicMetadata parseTopicMetadata(String response) {
        JsonNode root = tryParseJson(response);
        if (root == null || !root.isObject()) {
            return null;
        }
        return parseTopicMetadataNode(root);
    }

    private TopicMetadata parseTopicMetadataNode(JsonNode root) {
        TopicMetadata metadata = new TopicMetadata();
        metadata.setTitle(text(root, "title"));
        metadata.setSummary(text(root, "summary"));
        metadata.setExternalParty(text(root, "external_party", "externalParty"));
        metadata.setChannel(text(root, "channel"));
        metadata.setUrgency(text(root, "urgency"));
        metadata.setDeadline(text(root, "deadline"));
        metadata.setStatus(text(root, "status"));
        metadata.setTags(asTextList(root.get("tags")));
        metadata.setParticipants(asTextList(root.get("participants")));
        JsonNode items = root.get("action_items");
        if (items == null) items = root.get("actionItems");
        if (items != null && items.isArray()) {
            List<TopicActionItem> out = new ArrayList<>();
            for (JsonNode item : items) {
                if (!item.isObject()) continue;
                TopicActionItem ai = new TopicActionItem();
                ai.setTask(text(item, "task"));
                ai.setOwner(text(item, "owner"));
                ai.setOwnerUserId(text(item, "owner_user_id", "ownerUserId", "user_id", "userId", "slack_user_id", "slackUserId"));
                ai.setOwnerEmail(text(item, "owner_email", "ownerEmail", "email"));
                ai.setDueDate(text(item, "due_date", "dueDate"));
                ai.setStatus(text(item, "status"));
                ai.setPriority(text(item, "priority"));
                out.add(ai);
            }
            metadata.setActionItems(out);
        }
        return metadata;
    }

    private JsonNode tryParseJson(String response) {
        if (!StringUtils.hasText(response)) {
            return null;
        }
        String normalized = response.trim()
            .replace("```json", "")
            .replace("```", "")
            .replaceAll(",\\s*}", "}")
            .replaceAll(",\\s*]", "]")
            .trim();
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

    private List<String> asTextList(JsonNode node) {
        if (node == null) return List.of();
        if (node.isArray()) {
            List<String> out = new ArrayList<>();
            for (JsonNode v : node) {
                if (v != null && v.isTextual() && StringUtils.hasText(v.asText())) {
                    out.add(v.asText());
                }
            }
            return out;
        }
        return List.of();
    }

    private String text(JsonNode node, String... keys) {
        if (node == null || keys == null) return null;
        for (String k : keys) {
            JsonNode v = node.get(k);
            if (v != null && v.isTextual() && StringUtils.hasText(v.asText())) {
                return v.asText();
            }
        }
        return null;
    }

    private String stableTopicId(String workspaceId, String channelName, String messageId) {
        String basis = safe(workspaceId) + "|" + safe(channelName) + "|" + safe(messageId);
        return UUID.nameUUIDFromBytes(basis.getBytes(StandardCharsets.UTF_8)).toString();
    }

    private String buildMessageId(String tenantId, String workspaceId, String channelId, String threadTs, String messageTs) {
        String tid = safe(tenantId);
        String wid = safe(workspaceId);
        String cid = safe(channelId);
        String tts = StringUtils.hasText(threadTs) ? threadTs.trim() : safe(messageTs);
        String mts = safe(messageTs);
        return tid + ":" + wid + ":" + cid + ":" + tts + ":" + mts;
    }

    private String userLabel(IngestionUserDTO user) {
        if (user == null) return "";
        String name = StringUtils.hasText(user.getDisplayName()) ? user.getDisplayName()
            : StringUtils.hasText(user.getName()) ? user.getName() : "";
        String id = StringUtils.hasText(user.getSlackUserId()) ? user.getSlackUserId()
            : StringUtils.hasText(user.getUniqueUserId()) ? user.getUniqueUserId() : "";
        if (StringUtils.hasText(name) && StringUtils.hasText(id)) {
            return name + " (" + id + ")";
        }
        return StringUtils.hasText(id) ? id : name;
    }

    private String safe(String v) {
        return v == null ? "" : v.trim();
    }

    private String safeJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            return "{}";
        }
    }
}
