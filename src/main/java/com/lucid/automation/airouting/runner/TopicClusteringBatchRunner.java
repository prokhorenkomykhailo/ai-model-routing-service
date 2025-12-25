package com.lucid.automation.airouting.runner;

import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.Workspace;
import com.lucid.automation.airouting.service.TopicClusteringService;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    private final TopicClusteringService clusteringService;

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

    public TopicClusteringBatchRunner(TopicClusteringService clusteringService) {
        this.clusteringService = clusteringService;
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

        clusteringService.processBatch(ws, messages, metadata, batchNumber);
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
