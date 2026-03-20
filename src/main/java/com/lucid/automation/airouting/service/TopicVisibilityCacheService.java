package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.config.TopicVisibilityProperties;
import com.lucid.automation.common.dto.topic.TopicVisibilityEvent;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
public class TopicVisibilityCacheService {

    private final TopicVisibilityProperties properties;
    private final ConcurrentHashMap<String, CacheEntry> cache = new ConcurrentHashMap<>();

    public TopicVisibilityCacheService(TopicVisibilityProperties properties) {
        this.properties = properties;
    }

    public void update(String workspaceId, String userId, String topicId, String batchId, String channel) {
        if (!StringUtils.hasText(workspaceId) || !StringUtils.hasText(userId) || !StringUtils.hasText(topicId)) {
            return;
        }
        String key = key(workspaceId, userId);
        CacheEntry entry = cache.compute(key, (k, existing) -> {
            CacheEntry e = existing == null ? new CacheEntry() : existing;
            if (e.isExpired(properties.getTtlSeconds())) {
                e = new CacheEntry();
            }
            e.lastUpdatedAt = Instant.now();
            e.topicIds.add(topicId);
            if (StringUtils.hasText(batchId)) {
                e.batchIds.add(batchId);
            }
            if (StringUtils.hasText(channel)) {
                e.channels.add(channel);
            }
            e.enforceMaxTopics(properties.getMaxTopicsPerUser());
            return e;
        });
    }

    public TopicVisibilityEvent snapshotEvent(String workspaceId, String userId) {
        if (!StringUtils.hasText(workspaceId) || !StringUtils.hasText(userId)) {
            return null;
        }
        CacheEntry entry = cache.get(key(workspaceId, userId));
        if (entry == null || entry.isExpired(properties.getTtlSeconds())) {
            cache.remove(key(workspaceId, userId));
            return null;
        }

        TopicVisibilityEvent v = new TopicVisibilityEvent();
        v.setEventId(UUID.randomUUID().toString());
        v.setCreatedAt(Instant.now());
        v.setWorkspaceId(workspaceId);
        v.setUserId(userId);
        v.setTopicIds(new ArrayList<>(entry.topicIds));
        v.setBatchIds(new ArrayList<>(entry.batchIds));
        v.setChannels(new ArrayList<>(entry.channels));
        v.setRule("owns_open_action_item_and_member_of_channel");
        return v;
    }

    private String key(String workspaceId, String userId) {
        return workspaceId + ":" + userId;
    }

    private static final class CacheEntry {
        private Instant lastUpdatedAt = Instant.EPOCH;
        private LinkedHashSet<String> topicIds = new LinkedHashSet<>();
        private LinkedHashSet<String> batchIds = new LinkedHashSet<>();
        private LinkedHashSet<String> channels = new LinkedHashSet<>();

        private boolean isExpired(long ttlSeconds) {
            if (ttlSeconds <= 0) {
                return false;
            }
            return lastUpdatedAt.plusSeconds(ttlSeconds).isBefore(Instant.now());
        }

        private void enforceMaxTopics(int max) {
            if (max <= 0) {
                return;
            }
            while (topicIds.size() > max) {
                String first = topicIds.iterator().next();
                topicIds.remove(first);
            }
        }
    }
}

