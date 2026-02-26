package com.lucid.automation.airouting.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.genai.Client;
import com.google.genai.types.ContentEmbedding;
import com.google.genai.types.EmbedContentConfig;
import com.google.genai.types.EmbedContentResponse;
import com.lucid.automation.airouting.config.TopicEmbeddingProperties;
import com.lucid.automation.airouting.dto.topic.TopicMetadata;
import com.lucid.automation.airouting.dto.topic.TopicMetadataEvent;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
public class TopicEmbeddingStoreService {

    private static final Logger logger = LoggerFactory.getLogger(TopicEmbeddingStoreService.class);

    private final TopicEmbeddingProperties properties;
    private final ObjectMapper objectMapper;
    private final JdbcTemplate jdbc;

    private final Client geminiClient;
    private final boolean geminiAvailable;

    public TopicEmbeddingStoreService(
        TopicEmbeddingProperties properties,
        ObjectMapper objectMapper,
        @Qualifier("topicEmbeddingJdbcTemplate") Optional<JdbcTemplate> topicEmbeddingJdbcTemplate
    ) {
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.jdbc = topicEmbeddingJdbcTemplate.orElse(null);

        Client tempClient = null;
        boolean available = false;
        String apiKey = resolveApiKey();
        if (StringUtils.hasText(apiKey) && "gemini".equalsIgnoreCase(properties.getProvider())) {
            try {
                tempClient = Client.builder().apiKey(apiKey.trim()).build();
                available = true;
            } catch (Exception e) {
                logger.warn("Step4: failed to init Gemini embedding client: {}", e.getMessage());
            }
        }
        this.geminiClient = tempClient;
        this.geminiAvailable = available;

        if (jdbc != null && properties.getPgvector().isAutoDdl()) {
            ensureSchema();
        }
    }

    public void upsertFromMetadataEvent(TopicMetadataEvent event) {
        if (event == null || !StringUtils.hasText(event.getTopicId()) || event.getTopic() == null) {
            return;
        }
        if (jdbc == null) {
            logger.warn("Step4: pgvector jdbc not configured; set topic.embedding.pgvector.jdbc-url");
            return;
        }
        upsert(event.getTopicId(), event.getTopic(), event.getWorkspaceId(), event.getBatchId(), event.getProviderId(), event);
    }

    public void upsert(String topicId, TopicMetadata topic, String workspaceId, String batchId, String providerId) {
        upsert(topicId, topic, workspaceId, batchId, providerId, null);
    }

    private void upsert(String topicId,
                        TopicMetadata topic,
                        String workspaceId,
                        String batchId,
                        String providerId,
                        TopicMetadataEvent event) {
        if (!StringUtils.hasText(topicId) || topic == null) {
            return;
        }

        double[] vector = generateEmbedding(topic);
        String vectorLiteral = toVectorLiteral(vector);
        Map<String, Object> metadata = mergeWithExisting(topicId, metadataForRow(topic, event), event);
        String metadataJson = safeJson(metadata);

        String schema = properties.getPgvector().getSchema();
        String table = properties.getPgvector().getTable();

        String sql = """
            INSERT INTO %s.%s
              (topic_id, workspace_id, batch_id, embedding_provider, embedding_model, embedding, topic_metadata, created_at, updated_at)
            VALUES
              (?, ?, ?, ?, ?, ?::vector, ?::jsonb, now(), now())
            ON CONFLICT (topic_id)
            DO UPDATE SET
              workspace_id = EXCLUDED.workspace_id,
              batch_id = EXCLUDED.batch_id,
              embedding_provider = EXCLUDED.embedding_provider,
              embedding_model = EXCLUDED.embedding_model,
              embedding = EXCLUDED.embedding,
              topic_metadata = EXCLUDED.topic_metadata,
              updated_at = now()
            """.formatted(schema, table);

        jdbc.update(sql,
            topicId,
            nullToEmpty(workspaceId),
            nullToEmpty(batchId),
            nullToEmpty(properties.getProvider()),
            nullToEmpty(properties.getGeminiModel()),
            vectorLiteral,
            metadataJson
        );

        logger.info("Step4 upserted pgvector embedding topicId={} dim={} workspaceId={}", topicId, vector.length, workspaceId);
    }

    private Map<String, Object> mergeWithExisting(String topicId, Map<String, Object> next, TopicMetadataEvent event) {
        if (jdbc == null || !StringUtils.hasText(topicId)) {
            return next;
        }
        Map<String, Object> existing = loadExistingMetadata(topicId);
        if (existing.isEmpty()) {
            next.putIfAbsent("version", resolveEventVersion(event, 0, 1));
            return next;
        }

        int oldVersion = parseInt(existing.get("version"), 0);
        int nextVersion = resolveEventVersion(event, oldVersion, Math.max(1, oldVersion + 1));
        next.put("version", nextVersion);

        // Merge messageIds (keep unique and cap).
        List<String> mergedMessageIds = mergeStrings(existing.get("messageIds"), next.get("messageIds"), 500);
        if (!mergedMessageIds.isEmpty()) {
            next.put("messageIds", mergedMessageIds);
        }

        // Append to history (keep last N snapshots).
        List<Map<String, Object>> history = readHistory(existing.get("history"));
        history.add(snapshot(existing, oldVersion));
        int cap = 20;
        if (history.size() > cap) {
            history = history.subList(history.size() - cap, history.size());
        }
        next.put("history", history);
        next.put("historyLength", history.size());

        // Helpful pointers for debugging.
        next.put("previousVersion", oldVersion);
        next.put("lastUpsertedAt", Instant.now().toString());
        Object meta = event != null ? event.getMetadata() : null;
        if (meta instanceof Map<?, ?> m) {
            Object action = m.get("action");
            if (action != null) {
                next.put("lastAction", action.toString());
            }
        }
        return next;
    }

    private Map<String, Object> loadExistingMetadata(String topicId) {
        try {
            String schema = properties.getPgvector().getSchema();
            String table = properties.getPgvector().getTable();
            String sql = "SELECT topic_metadata::text AS meta FROM " + schema + "." + table + " WHERE topic_id::text = ?";
            List<String> rows = jdbc.query(sql, (rs, i) -> rs.getString("meta"), topicId);
            if (rows.isEmpty() || !StringUtils.hasText(rows.get(0))) {
                return Map.of();
            }
            return objectMapper.readValue(rows.get(0), Map.class);
        } catch (Exception e) {
            return Map.of();
        }
    }

    private int resolveEventVersion(TopicMetadataEvent event, int oldVersion, int fallback) {
        if (event == null || !(event.getMetadata() instanceof Map<?, ?> m)) {
            return fallback;
        }
        Object v = m.get("topicVersion");
        int parsed = parseInt(v, 0);
        if (parsed > 0) {
            return parsed;
        }
        return fallback;
    }

    private int parseInt(Object v, int fallback) {
        if (v == null) return fallback;
        try {
            return Integer.parseInt(v.toString().trim());
        } catch (Exception e) {
            return fallback;
        }
    }

    private List<Map<String, Object>> readHistory(Object v) {
        if (!(v instanceof List<?> list)) {
            return new ArrayList<>();
        }
        List<Map<String, Object>> out = new ArrayList<>();
        for (Object item : list) {
            if (item instanceof Map<?, ?> m) {
                out.add(new LinkedHashMap<>((Map<String, Object>) m));
            }
        }
        return out;
    }

    private Map<String, Object> snapshot(Map<String, Object> meta, int version) {
        Map<String, Object> s = new LinkedHashMap<>();
        s.put("version", version);
        s.put("recordedAt", Instant.now().toString());
        s.put("title", meta.get("title"));
        s.put("summary", meta.get("summary"));
        s.put("channel", meta.get("channel"));
        s.put("externalParty", meta.get("externalParty"));
        s.put("participantsCount", sizeOfList(meta.get("participants")));
        s.put("actionItemsCount", sizeOfList(meta.get("actionItems")));
        s.put("tagsCount", sizeOfList(meta.get("tags")));
        return s;
    }

    private int sizeOfList(Object v) {
        return v instanceof List<?> list ? list.size() : 0;
    }

    private List<String> mergeStrings(Object existing, Object next, int cap) {
        Set<String> out = new LinkedHashSet<>();
        if (existing instanceof List<?> a) {
            for (Object o : a) if (o != null && StringUtils.hasText(o.toString())) out.add(o.toString());
        }
        if (next instanceof List<?> b) {
            for (Object o : b) if (o != null && StringUtils.hasText(o.toString())) out.add(o.toString());
        }
        List<String> merged = new ArrayList<>(out);
        if (merged.size() > cap) {
            return merged.subList(merged.size() - cap, merged.size());
        }
        return merged;
    }

    public List<ScoredTopic> search(String workspaceId, TopicMetadata queryTopic, int topK) {
        if (!StringUtils.hasText(workspaceId) || queryTopic == null) {
            return List.of();
        }
        return search(workspaceId, generateEmbedding(queryTopic), topK);
    }

    public List<ScoredTopic> search(String workspaceId, double[] queryVector, int topK) {
        if (jdbc == null || !StringUtils.hasText(workspaceId) || queryVector == null || queryVector.length == 0) {
            return List.of();
        }

        int limit = topK > 0 ? topK : properties.getPgvector().getSearchTopKDefault();
        String schema = properties.getPgvector().getSchema();
        String table = properties.getPgvector().getTable();
        String queryLiteral = toVectorLiteral(normalizeCopy(queryVector));

        String sql = """
            WITH q AS (SELECT CAST(? AS vector) AS qv)
            SELECT t.topic_id::text AS topic_id,
                   1.0 - (t.embedding <=> q.qv) AS score
              FROM %s.%s t, q
             WHERE t.workspace_id = ?
             ORDER BY t.embedding <=> q.qv
             LIMIT ?
            """.formatted(schema, table);

        try {
            return jdbc.query(sql, (rs, rowNum) ->
                    new ScoredTopic(rs.getString("topic_id"), rs.getDouble("score")),
                queryLiteral,
                workspaceId,
                limit
            );
        } catch (Exception e) {
            logger.warn("Step4 search failed (workspaceId={}): {}", workspaceId, e.getMessage());
            return List.of();
        }
    }

    private void ensureSchema() {
        String schema = properties.getPgvector().getSchema();
        String table = properties.getPgvector().getTable();
        int dim = Math.max(1, properties.getDimension());

        jdbc.execute("CREATE SCHEMA IF NOT EXISTS " + schema);
        jdbc.execute("CREATE EXTENSION IF NOT EXISTS vector");

        String ddl = """
            CREATE TABLE IF NOT EXISTS %s.%s (
              topic_id text PRIMARY KEY,
              workspace_id text NOT NULL,
              batch_id text,
              embedding_provider text,
              embedding_model text,
              embedding vector(%d) NOT NULL,
              topic_metadata jsonb,
              created_at timestamptz NOT NULL DEFAULT now(),
              updated_at timestamptz NOT NULL DEFAULT now()
            )
            """.formatted(schema, table, dim);
        jdbc.execute(ddl);
    }

    private double[] generateEmbedding(TopicMetadata topic) {
        int dim = Math.max(1, properties.getDimension());
        String input = buildEmbeddingText(topic);

        if ("gemini".equalsIgnoreCase(properties.getProvider()) && geminiAvailable) {
            try {
                EmbedContentConfig config = EmbedContentConfig.builder().build();
                EmbedContentResponse response = geminiClient.models.embedContent(properties.getGeminiModel(), input, config);
                List<ContentEmbedding> embeddings = response.embeddings().orElse(List.of());
                if (!embeddings.isEmpty() && embeddings.get(0).values().isPresent()) {
                    List<Float> values = embeddings.get(0).values().get();
                    double[] vec = new double[Math.min(values.size(), dim)];
                    for (int i = 0; i < vec.length; i++) {
                        vec[i] = values.get(i);
                    }
                    double[] fixed = fixDimension(vec, dim);
                    normalizeInPlace(fixed);
                    return fixed;
                }
            } catch (Exception e) {
                logger.warn("Step4: Gemini embed failed, falling back to deterministic embedding: {}", e.getMessage());
            }
        }

        double[] fallback = deterministicEmbedding(input, dim);
        normalizeInPlace(fallback);
        return fallback;
    }

    private String buildEmbeddingText(TopicMetadata topic) {
        return String.join("\n",
            safe(topic.getExternalParty()),
            safe(topic.getChannel()),
            safe(topic.getTitle()),
            safe(topic.getSummary()),
            safe(topic.getSuggestedAction()),
            "tags: " + join(topic.getTags())
        ).trim();
    }

    private Map<String, Object> metadataForRow(TopicMetadata topic, TopicMetadataEvent event) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("title", topic.getTitle());
        m.put("summary", topic.getSummary());
        m.put("externalParty", topic.getExternalParty());
        m.put("priority", topic.getPriority());
        m.put("reason", topic.getReason());
        m.put("suggestedAction", topic.getSuggestedAction());
        m.put("situation", topic.getSituation());
        m.put("impact", topic.getImpact());
        m.put("proposedSolution", topic.getProposedSolution());
        m.put("decisionNeeded", topic.getDecisionNeeded());
        m.put("participants", topic.getParticipants());
        m.put("actionItems", topic.getActionItems());
        m.put("tags", topic.getTags());
        m.put("channel", topic.getChannel());
        m.put("deadline", topic.getDeadline());
        m.put("urgency", topic.getUrgency());
        m.put("status", topic.getStatus());

        if (event != null) {
            if (event.getMessageIds() != null && !event.getMessageIds().isEmpty()) {
                m.put("messageIds", event.getMessageIds());
            }
            Object metaObj = event.getMetadata();
            if (metaObj instanceof Map<?, ?> meta) {
                Object threadTs = meta.get("threadTs");
                if (threadTs != null && StringUtils.hasText(threadTs.toString())) {
                    m.put("threadTs", threadTs.toString());
                }
                Object channelId = meta.get("channelId");
                if (channelId != null && StringUtils.hasText(channelId.toString())) {
                    m.put("channelId", channelId.toString());
                }
            }
        }

        return m;
    }

    private String toVectorLiteral(double[] vector) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (int i = 0; i < vector.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(Double.toString(vector[i]));
        }
        sb.append(']');
        return sb.toString();
    }

    private double[] deterministicEmbedding(String text, int dim) {
        // Feature-hashing fallback embedding (deterministic, similarity-preserving for shared tokens).
        // This keeps Step 6 update behavior reasonable even when the external embedding API is unavailable.
        String normalized = (text == null ? "" : text).toLowerCase().replaceAll("[^a-z0-9@# ]", " ");
        String[] tokens = normalized.trim().split("\\s+");

        double[] vec = new double[dim];
        for (String token : tokens) {
            if (token.isBlank() || token.length() < 2) {
                continue;
            }
            long h = mix64(token.hashCode() * 0x9E3779B97F4A7C15L);
            int idx = (int) (Math.floorMod(h, dim));
            double sign = ((h >>> 63) == 0) ? 1.0 : -1.0;
            vec[idx] += sign;
        }
        return vec;
    }

    private double[] fixDimension(double[] vec, int dim) {
        if (vec.length == dim) {
            return vec;
        }
        double[] out = new double[dim];
        int copy = Math.min(vec.length, dim);
        System.arraycopy(vec, 0, out, 0, copy);
        return out;
    }

    private double[] normalizeCopy(double[] v) {
        double[] out = v.clone();
        normalizeInPlace(out);
        return out;
    }

    private void normalizeInPlace(double[] v) {
        double sum = 0;
        for (double x : v) {
            sum += x * x;
        }
        double norm = Math.sqrt(sum);
        if (norm == 0) return;
        for (int i = 0; i < v.length; i++) {
            v[i] /= norm;
        }
    }

    private long mix64(long z) {
        z = (z ^ (z >>> 33)) * 0xff51afd7ed558ccdL;
        z = (z ^ (z >>> 33)) * 0xc4ceb9fe1a85ec53L;
        return z ^ (z >>> 33);
    }

    private String join(List<String> values) {
        if (values == null || values.isEmpty()) {
            return "";
        }
        return values.stream()
            .filter(Objects::nonNull)
            .map(String::trim)
            .filter(StringUtils::hasText)
            .limit(50)
            .reduce((a, b) -> a + ", " + b)
            .orElse("");
    }

    private String safe(String s) {
        return s == null ? "" : s.trim();
    }

    private String nullToEmpty(String s) {
        return s == null ? "" : s;
    }

    private String safeJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            return "{}";
        }
    }

    private String resolveApiKey() {
        String env = System.getenv("GEMINI_API_KEY");
        if (StringUtils.hasText(env)) return env;
        env = System.getenv("GOOGLE_API_KEY");
        if (StringUtils.hasText(env)) return env;
        return null;
    }

    public record ScoredTopic(String topicId, double score) {}
}
