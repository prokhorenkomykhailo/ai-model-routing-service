package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.dto.topic.TopicClusterDraftEvent;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Lightweight cache for draft events to support quick retrieval/observability.
 * In production this could be backed by Redis; here we keep an in-memory store
 * to unblock Step 1 and unit testing.
 */
@Service
public class TopicDraftCacheService {

    private static final Logger logger = LoggerFactory.getLogger(TopicDraftCacheService.class);
    private final Map<String, CachedDraft> cache = new ConcurrentHashMap<>();

    public void cacheDraft(TopicClusterDraftEvent event) {
        if (event == null || event.getBatchId() == null) {
            return;
        }
        cache.put(event.getBatchId(), new CachedDraft(event, Instant.now()));
        logger.info("🗂️ Cached topic draft for batch {}", event.getBatchId());
    }

    public Optional<TopicClusterDraftEvent> getDraft(String batchId) {
        return Optional.ofNullable(cache.get(batchId)).map(CachedDraft::event);
    }

    private record CachedDraft(TopicClusterDraftEvent event, Instant cachedAt) {}
}
