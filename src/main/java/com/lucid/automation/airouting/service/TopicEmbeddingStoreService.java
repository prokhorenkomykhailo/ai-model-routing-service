package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.dto.topic.TopicMetadata;
import com.lucid.automation.airouting.dto.topic.TopicMetadataEvent;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
public class TopicEmbeddingStoreService {

    private static final Logger logger = LoggerFactory.getLogger(TopicEmbeddingStoreService.class);

    private final RedisTemplate<String, Object> redisTemplate;

    @Value("${topic.embedding.redis-prefix:topic_embedding:}")
    private String redisPrefix;

    @Value("${topic.embedding.dimension:768}")
    private int dimension;

    @Value("${topic.embedding.ttl-seconds:86400}")
    private long ttlSeconds;

    public TopicEmbeddingStoreService(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void upsertFromMetadataEvent(TopicMetadataEvent event) {
        if (event == null || !StringUtils.hasText(event.getTopicId()) || event.getTopic() == null) {
            return;
        }
        upsert(event.getTopicId(), event.getTopic(), event.getWorkspaceId(), event.getBatchId());
    }

    public void upsert(String topicId, TopicMetadata topic, String workspaceId, String batchId) {
        if (!StringUtils.hasText(topicId) || topic == null) {
            return;
        }

        double[] vector = embedTopic(topic, dimension);
        String key = redisPrefix + topicId;

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("topicId", topicId);
        payload.put("workspaceId", workspaceId);
        payload.put("batchId", batchId);
        payload.put("dimension", dimension);
        payload.put("vector", vector);
        payload.put("createdAt", Instant.now().toString());

        redisTemplate.opsForValue().set(key, payload, Duration.ofSeconds(ttlSeconds));
        logger.info("Step4 upserted topic embedding for topicId={} dim={} ttlSeconds={}", topicId, dimension, ttlSeconds);
    }

    public Map<String, Object> get(String topicId) {
        if (!StringUtils.hasText(topicId)) {
            return null;
        }
        Object value = redisTemplate.opsForValue().get(redisPrefix + topicId);
        if (value instanceof Map<?, ?> map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> typed = (Map<String, Object>) map;
            return typed;
        }
        return null;
    }

    public double cosine(double[] a, double[] b) {
        if (a == null || b == null || a.length == 0 || b.length == 0) {
            return 0.0;
        }
        int len = Math.min(a.length, b.length);
        double dot = 0, na = 0, nb = 0;
        for (int i = 0; i < len; i++) {
            dot += a[i] * b[i];
            na += a[i] * a[i];
            nb += b[i] * b[i];
        }
        double denom = Math.sqrt(na) * Math.sqrt(nb);
        return denom == 0 ? 0.0 : dot / denom;
    }

    private double[] embedTopic(TopicMetadata topic, int dim) {
        String basis = String.join("|",
            safe(topic.getExternalParty()),
            safe(topic.getChannel()),
            safe(topic.getTitle()),
            safe(topic.getSummary()),
            join(topic.getTags()),
            join(topic.getParticipants())
        );

        byte[] seedBytes = basis.getBytes(StandardCharsets.UTF_8);
        UUID seed = UUID.nameUUIDFromBytes(seedBytes);

        long s0 = seed.getMostSignificantBits();
        long s1 = seed.getLeastSignificantBits();

        double[] vec = new double[Math.max(1, dim)];
        for (int i = 0; i < vec.length; i++) {
            long x = mix64(s0 + i * 0x9e3779b97f4a7c15L) ^ mix64(s1 - i * 0xC2B2AE3D27D4EB4FL);
            double v = ((x >>> 11) * 0x1.0p-53) * 2.0 - 1.0;
            vec[i] = v;
        }
        normalizeInPlace(vec);
        return vec;
    }

    private void normalizeInPlace(double[] v) {
        double sum = 0;
        for (double x : v) {
            sum += x * x;
        }
        double norm = Math.sqrt(sum);
        if (norm == 0) {
            return;
        }
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
            .reduce((a, b) -> a + "," + b)
            .orElse("");
    }

    private String safe(String s) {
        return s == null ? "" : s.trim();
    }
}
