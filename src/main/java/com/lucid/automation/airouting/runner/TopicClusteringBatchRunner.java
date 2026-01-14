package com.lucid.automation.airouting.runner;

import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.Workspace;
import com.lucid.automation.airouting.dto.topic.TopicClusterDraft;
import com.lucid.automation.airouting.dto.topic.TopicClusterDraftEvent;
import com.lucid.automation.airouting.producer.TopicDraftProducer;
import com.lucid.automation.airouting.service.TopicDraftCacheService;
import com.lucid.automation.airouting.service.TopicClusteringService;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * Optional one-shot runner that reads a CSV of Slack messages and triggers Step 1 clustering.
 * Enable by setting topic.clustering.runner.enabled=true and providing csv/workspace/tenant props.
 */
@Component
@ConditionalOnProperty(name = "topic.clustering.runner.enabled", havingValue = "true")
public class TopicClusteringBatchRunner implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(TopicClusteringBatchRunner.class);

    private static final List<String> KNOWN_PROJECTS = List.of(
        "EcoBloom", "FitFusion", "TechNova", "GreenScape", "UrbanEdge"
    );
    private static final Pattern WORD_SPLIT = Pattern.compile("\\s+");

    private final TopicClusteringService clusteringService;
    private final TopicDraftProducer topicDraftProducer;
    private final TopicDraftCacheService topicDraftCacheService;

    @Value("${topic.clustering.runner.csv-path:}")
    private String csvPath;

    @Value("${topic.clustering.runner.workspace-id:ws-local}")
    private String workspaceId;

    @Value("${topic.clustering.runner.workspace-name:Local Workspace}")
    private String workspaceName;

    @Value("${topic.clustering.runner.tenant-id:tenant-local}")
    private String tenantId;

    @Value("${topic.clustering.runner.batch-number:1}")
    private int batchNumber;

    @Value("${topic.clustering.runner.offline-enabled:false}")
    private boolean offlineEnabled;

    public TopicClusteringBatchRunner(TopicClusteringService clusteringService,
                                     TopicDraftProducer topicDraftProducer,
                                     TopicDraftCacheService topicDraftCacheService) {
        this.clusteringService = clusteringService;
        this.topicDraftProducer = topicDraftProducer;
        this.topicDraftCacheService = topicDraftCacheService;
    }

    @Override
    public void run(String... args) throws Exception {
        if (!StringUtils.hasText(csvPath)) {
            logger.warn("Skipping TopicClusteringBatchRunner: csv-path not provided");
            return;
        }

        Path path = Path.of(csvPath);
        if (!Files.exists(path)) {
            logger.error("CSV path does not exist: {}", csvPath);
            return;
        }

        List<SlackMessage> messages = readCsv(path);
        logger.info("Loaded {} messages from {}", messages.size(), csvPath);

        Workspace ws = new Workspace();
        ws.setId(workspaceId);
        ws.setName(workspaceName);
        ws.setTenantId(tenantId);
        ws.setDeemergeUserId("runner-user");

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("csvPath", csvPath);
        metadata.put("loadedAt", Instant.now().toString());

        if (offlineEnabled) {
            TopicClusterDraftEvent event = buildOfflineDraftEvent(ws, messages, metadata, batchNumber);
            topicDraftProducer.publishDraft(event);
            topicDraftCacheService.cacheDraft(event);
            logger.info("✅ Offline runner emitted {} topic drafts for workspace {} batch {}",
                event.getClusterCount(), ws.getId(), event.getBatchId());
            return;
        }

        try {
            clusteringService.processBatch(ws, messages, metadata, batchNumber);
        } catch (Exception e) {
            logger.error("Topic clustering runner failed: {}", e.getMessage(), e);
        }
    }

    private TopicClusterDraftEvent buildOfflineDraftEvent(Workspace workspace,
                                                         List<SlackMessage> messages,
                                                         Map<String, Object> metadata,
                                                         int batchNumber) {
        Map<String, List<SlackMessage>> buckets = new HashMap<>();
        for (SlackMessage msg : messages) {
            String bucketKey = bucketKey(msg);
            buckets.computeIfAbsent(bucketKey, k -> new ArrayList<>()).add(msg);
        }

        List<TopicClusterDraft> clusters = new ArrayList<>();
        int idx = 1;
        for (Map.Entry<String, List<SlackMessage>> entry : buckets.entrySet()) {
            String key = entry.getKey();
            List<SlackMessage> bucketMessages = entry.getValue();

            TopicClusterDraft draft = new TopicClusterDraft();
            draft.setClusterId("offline_" + UUID.nameUUIDFromBytes(key.getBytes(StandardCharsets.UTF_8)));
            draft.setChannel(bucketMessages.get(0).getChannelName());
            draft.setThreadId(bucketMessages.get(0).getThreadTs());
            draft.setParticipants(bucketMessages.stream()
                .map(SlackMessage::getDisplayName)
                .filter(StringUtils::hasText)
                .distinct()
                .toList());
            draft.setMessageIds(bucketMessages.stream()
                .map(SlackMessage::getId)
                .filter(StringUtils::hasText)
                .toList());
            draft.setDraftTitle(offlineTitle(bucketMessages, idx++));
            clusters.add(draft);
        }

        TopicClusterDraftEvent event = new TopicClusterDraftEvent();
        event.setEventId(UUID.randomUUID().toString());
        event.setWorkspaceId(workspace.getId());
        event.setBatchId(workspace.getId() + ":batch_" + batchNumber);
        event.setProviderId("offline");
        event.setPromptVersion("offline-v1");
        event.setCreatedAt(Instant.now());
        event.setMetadata(metadata);
        event.setClusters(clusters);
        event.setClusterCount(clusters.size());
        return event;
    }

    private String bucketKey(SlackMessage msg) {
        if (msg == null) {
            return "unknown";
        }
        if (StringUtils.hasText(msg.getThreadTs())) {
            return "thread:" + msg.getThreadTs();
        }

        String channel = StringUtils.hasText(msg.getChannelName()) ? msg.getChannelName() : safe(msg.getChannelId());
        String project = detectProject(msg.getText());
        if (project != null) {
            return "project:" + project + "|channel:" + channel;
        }
        return "channel:" + channel;
    }

    private String offlineTitle(List<SlackMessage> messages, int idx) {
        if (messages == null || messages.isEmpty()) {
            return "Offline cluster " + idx;
        }
        SlackMessage first = messages.get(0);
        String project = detectProject(first.getText());
        if (project != null) {
            return project + " discussion";
        }
        String channel = StringUtils.hasText(first.getChannelName()) ? first.getChannelName() : safe(first.getChannelId());
        String preview = safe(first.getText());
        String[] words = WORD_SPLIT.split(preview.trim());
        String head = String.join(" ", Arrays.stream(words).limit(6).toList());
        return StringUtils.hasText(head) ? channel + ": " + head : "Offline cluster " + idx;
    }

    private String detectProject(String text) {
        if (!StringUtils.hasText(text)) {
            return null;
        }
        for (String project : KNOWN_PROJECTS) {
            if (text.contains(project)) {
                return project;
            }
        }
        return null;
    }

    private String safe(String value) {
        return value == null ? "" : value.trim();
    }

    private List<SlackMessage> readCsv(Path path) throws Exception {
        List<SlackMessage> list = new ArrayList<>();
        List<String> lines = Files.readAllLines(path);
        if (lines.isEmpty()) {
            return list;
        }
        // skip header
        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i);
            List<String> cols = parseCsvLine(line);
            if (cols.size() < 6) {
                continue;
            }
            SlackMessage msg = new SlackMessage();
            msg.setId(String.valueOf(i));
            msg.setChannel(cols.get(0));
            msg.setChannelName(cols.get(0));
            msg.setDisplayName(cols.get(1));
            msg.setUniqueUserId(cols.get(2));
            msg.setUser(cols.get(2));
            msg.setThreadTs(nullIfNone(cols.get(5)));
            msg.setThreadId(nullIfNone(cols.get(5)));
            msg.setText(cols.get(4));
            msg.setTimestamp(parseTimestamp(cols.get(3)));
            list.add(msg);
        }
        return list;
    }

    private LocalDateTime parseTimestamp(String value) {
        try {
            long epochMillis = Long.parseLong(value);
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneId.systemDefault());
        } catch (Exception e) {
            return LocalDateTime.now();
        }
    }

    private String nullIfNone(String val) {
        if (!StringUtils.hasText(val)) {
            return null;
        }
        if ("None".equalsIgnoreCase(val)) {
            return null;
        }
        return val;
    }

    /**
     * Minimal CSV parser that handles quoted values with commas.
     */
    private List<String> parseCsvLine(String line) {
        List<String> result = new ArrayList<>();
        if (line == null) {
            return result;
        }
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '\"') {
                inQuotes = !inQuotes;
                continue;
            }
            if (c == ',' && !inQuotes) {
                result.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        result.add(current.toString());
        return result;
    }
}
