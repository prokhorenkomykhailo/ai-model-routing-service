package com.lucid.automation.airouting.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucid.automation.airouting.config.TopicVisibilityProperties;
import com.lucid.automation.airouting.dto.topic.TopicMetadata;
import com.lucid.automation.airouting.dto.topic.TopicMetadataEvent;
import com.lucid.automation.airouting.dto.topic.TopicActionItem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
public class TopicVisibilityBatchReportService {

    private final TopicVisibilityProperties properties;
    private final ObjectMapper objectMapper;

    private final ConcurrentHashMap<String, BatchState> batches = new ConcurrentHashMap<>();

    public TopicVisibilityBatchReportService(TopicVisibilityProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    public void update(TopicMetadataEvent event, TopicVisibilityService.VisibilityEvaluation evaluation) {
        if (!properties.isBatchReportEnabled() || event == null || evaluation == null) {
            return;
        }
        String batchId = StringUtils.hasText(event.getBatchId()) ? event.getBatchId() : "unknown-batch";
        String workspaceId = StringUtils.hasText(event.getWorkspaceId()) ? event.getWorkspaceId() : "unknown-workspace";
        BatchState state = batches.computeIfAbsent(key(workspaceId, batchId), k -> new BatchState(workspaceId, batchId));
        state.lastUpdatedAt = Instant.now();

        String topicId = event.getTopicId();
        TopicMetadata topic = event.getTopic();
        state.topics.put(topicId, new TopicRow(
            topicId,
            safe(topic != null ? topic.getTitle() : null),
            safe(topic != null ? topic.getSummary() : null),
            safe(topic != null ? topic.getChannel() : null),
            topic != null && topic.getTags() != null ? List.copyOf(topic.getTags()) : List.of(),
            topic != null && topic.getParticipants() != null ? List.copyOf(topic.getParticipants()) : List.of(),
            topic != null && topic.getActionItems() != null ? copyActionItems(topic.getActionItems()) : List.of()
        ));

        Set<String> users = new LinkedHashSet<>();
        for (TopicVisibilityService.UserTopicVisibility m : evaluation.matches()) {
            users.add(m.userId());
            state.userToTopics.computeIfAbsent(m.userId(), u -> new LinkedHashSet<>()).add(topicId);
        }
        state.topicToUsers.put(topicId, users);
        state.topicSkipReasons.put(topicId, evaluation.skipReason().name());
    }

    public Path writeSnapshot(String workspaceId, String batchId) {
        if (!properties.isBatchReportEnabled()) {
            return null;
        }
        if (!StringUtils.hasText(workspaceId) || !StringUtils.hasText(batchId)) {
            return null;
        }
        BatchState state = batches.get(key(workspaceId, batchId));
        if (state == null) {
            return null;
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("workspaceId", state.workspaceId);
        out.put("batchId", state.batchId);
        out.put("generatedAt", Instant.now().toString());
        out.put("lastUpdatedAt", state.lastUpdatedAt != null ? state.lastUpdatedAt.toString() : null);

        int topicsProcessed = state.topics.size();
        int topicsZeroVisibility = (int) state.topicToUsers.values().stream().filter(Set::isEmpty).count();

        double avgUsersPerTopic = topicsProcessed == 0 ? 0.0
            : state.topicToUsers.values().stream().mapToInt(Set::size).average().orElse(0.0);
        double avgTopicsPerUser = state.userToTopics.isEmpty() ? 0.0
            : state.userToTopics.values().stream().mapToInt(Set::size).average().orElse(0.0);

        out.put("topicsProcessed", topicsProcessed);
        out.put("topicsWithZeroVisibility", topicsZeroVisibility);
        out.put("avgUsersPerTopic", avgUsersPerTopic);
        out.put("avgTopicsPerUser", avgTopicsPerUser);
        out.put("rule", properties.getRule());
        out.put("strictChannelMembership", properties.isStrictChannelMembership());

        List<Map<String, Object>> perTopic = new ArrayList<>();
        for (TopicRow row : state.topics.values().stream()
            .sorted(Comparator.comparing(r -> r.topicId))
            .toList()) {
            Map<String, Object> t = new LinkedHashMap<>();
            t.put("topicId", row.topicId);
            t.put("title", row.title);
            t.put("summary", row.summary);
            t.put("channel", row.channel);
            t.put("tags", row.tags);
            t.put("participants", row.participants);
            t.put("actionItems", row.actionItems);
            Set<String> visibleUsers = state.topicToUsers.getOrDefault(row.topicId, Set.of());
            t.put("visibleUserCount", visibleUsers.size());
            t.put("visibleUsers", visibleUsers.stream().limit(200).toList());
            t.put("skipReason", state.topicSkipReasons.getOrDefault(row.topicId, "UNKNOWN"));
            perTopic.add(t);
        }
        out.put("perTopic", perTopic);

        List<Map<String, Object>> perUser = new ArrayList<>();
        for (Map.Entry<String, Set<String>> e : state.userToTopics.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .toList()) {
            Map<String, Object> u = new LinkedHashMap<>();
            u.put("userKey", e.getKey());
            u.put("topicCount", e.getValue().size());
            List<Map<String, Object>> topics = new ArrayList<>();
            for (String topicId : e.getValue()) {
                TopicRow row = state.topics.get(topicId);
                Map<String, Object> tr = new LinkedHashMap<>();
                tr.put("topicId", topicId);
                tr.put("title", row != null ? row.title : "");
                tr.put("channel", row != null ? row.channel : "");
                topics.add(tr);
            }
            u.put("topics", topics);
            perUser.add(u);
        }
        out.put("perUser", perUser);

        try {
            Path dir = Path.of(StringUtils.hasText(properties.getReportDir()) ? properties.getReportDir() : "logs");
            Files.createDirectories(dir);
            Path file = dir.resolve("step5_summary_" + sanitize(state.workspaceId) + "_" + sanitize(state.batchId) + ".json");
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), out);
            return file;
        } catch (Exception e) {
            return null;
        }
    }

    private String safe(String v) {
        return v == null ? "" : v;
    }

    private String key(String workspaceId, String batchId) {
        return workspaceId + "||" + batchId;
    }

    private String sanitize(String v) {
        if (!StringUtils.hasText(v)) return "unknown";
        return v.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    private static final class BatchState {
        private final String workspaceId;
        private final String batchId;
        private volatile Instant lastUpdatedAt;
        private final ConcurrentHashMap<String, TopicRow> topics = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, Set<String>> topicToUsers = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, Set<String>> userToTopics = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, String> topicSkipReasons = new ConcurrentHashMap<>();

        private BatchState(String workspaceId, String batchId) {
            this.workspaceId = workspaceId;
            this.batchId = batchId;
        }
    }

    private record TopicRow(
        String topicId,
        String title,
        String summary,
        String channel,
        List<String> tags,
        List<String> participants,
        List<Map<String, Object>> actionItems
    ) {}

    private List<Map<String, Object>> copyActionItems(List<TopicActionItem> items) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (TopicActionItem ai : items) {
            if (ai == null) continue;
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("task", ai.getTask());
            m.put("owner", ai.getOwner());
            m.put("ownerUserId", ai.getOwnerUserId());
            m.put("ownerEmail", ai.getOwnerEmail());
            m.put("dueDate", ai.getDueDate());
            m.put("status", ai.getStatus());
            m.put("priority", ai.getPriority());
            out.add(m);
        }
        return out;
    }
}
