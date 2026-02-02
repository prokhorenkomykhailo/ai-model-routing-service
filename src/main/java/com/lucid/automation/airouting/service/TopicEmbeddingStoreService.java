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
        upsert(event.getTopicId(), event.getTopic(), event.getWorkspaceId(), event.getBatchId(), event.getProviderId());
    }

    public void upsert(String topicId, TopicMetadata topic, String workspaceId, String batchId, String providerId) {
        if (!StringUtils.hasText(topicId) || topic == null) {
            return;
        }

        double[] vector = generateEmbedding(topic);
        String vectorLiteral = toVectorLiteral(vector);
        String metadataJson = safeJson(metadataForRow(topic));

        String schema = properties.getPgvector().getSchema();
        String table = properties.getPgvector().getTable();

        String sql = """
            INSERT INTO %s.%s
              (topic_id, workspace_id, batch_id, embedding_provider, embedding_model, embedding, topic_metadata, created_at, updated_at)
            VALUES
              (?::uuid, ?, ?, ?, ?, ?::vector, ?::jsonb, now(), now())
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
            SELECT topic_id::text AS topic_id,
                   1.0 - (embedding <=> ?::vector) AS score
              FROM %s.%s
             WHERE workspace_id = ?
             ORDER BY embedding <=> ?::vector
             LIMIT ?
            """.formatted(schema, table);

        return jdbc.query(sql, (rs, rowNum) ->
                new ScoredTopic(rs.getString("topic_id"), rs.getDouble("score")),
            queryLiteral,
            workspaceId,
            queryLiteral,
            limit
        );
    }

    private void ensureSchema() {
        String schema = properties.getPgvector().getSchema();
        String table = properties.getPgvector().getTable();
        int dim = Math.max(1, properties.getDimension());

        jdbc.execute("CREATE SCHEMA IF NOT EXISTS " + schema);
        jdbc.execute("CREATE EXTENSION IF NOT EXISTS vector");

        String ddl = """
            CREATE TABLE IF NOT EXISTS %s.%s (
              topic_id uuid PRIMARY KEY,
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
            "participants: " + join(topic.getParticipants()),
            "tags: " + join(topic.getTags())
        ).trim();
    }

    private Map<String, Object> metadataForRow(TopicMetadata topic) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("title", topic.getTitle());
        m.put("summary", topic.getSummary());
        m.put("externalParty", topic.getExternalParty());
        m.put("participants", topic.getParticipants());
        m.put("tags", topic.getTags());
        m.put("channel", topic.getChannel());
        m.put("deadline", topic.getDeadline());
        m.put("urgency", topic.getUrgency());
        m.put("status", topic.getStatus());
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
        UUID seed = UUID.nameUUIDFromBytes((text == null ? "" : text).getBytes(StandardCharsets.UTF_8));
        long s0 = seed.getMostSignificantBits();
        long s1 = seed.getLeastSignificantBits();
        double[] vec = new double[dim];
        for (int i = 0; i < dim; i++) {
            long x = mix64(s0 + i * 0x9e3779b97f4a7c15L) ^ mix64(s1 - i * 0xC2B2AE3D27D4EB4FL);
            vec[i] = ((x >>> 11) * 0x1.0p-53) * 2.0 - 1.0;
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

