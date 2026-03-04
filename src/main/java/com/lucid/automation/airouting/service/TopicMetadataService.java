package com.lucid.automation.airouting.service;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucid.automation.airouting.config.TopicMetadataProperties;
import com.lucid.automation.airouting.dto.topic.TopicActionItem;
import com.lucid.automation.airouting.dto.topic.TopicClusterRefined;
import com.lucid.automation.airouting.dto.topic.TopicClusterRefinedEvent;
import com.lucid.automation.airouting.dto.topic.TopicMetadata;
import com.lucid.automation.airouting.dto.topic.TopicMetadataEvent;
import com.lucid.automation.airouting.model.AITaskType;
import com.lucid.automation.airouting.model.Message;
import com.lucid.automation.airouting.model.Workspace;
import com.lucid.automation.airouting.producer.TopicMetadataProducer;
import com.lucid.automation.airouting.provider.AIProvider;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
public class TopicMetadataService {

    private static final Logger logger = LoggerFactory.getLogger(TopicMetadataService.class);

    private final TopicMetadataPromptBuilder promptBuilder;
    private final AIProviderRouterService providerRouterService;
    private final TopicMetadataProducer producer;
    private final MessageService messageService;
    private final PendingResponseSignalService pendingResponseSignalService;
    private final ObjectMapper objectMapper;
    private final TopicMetadataProperties properties;

    private static final Pattern CSV_SPLIT = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
    private volatile Map<Integer, CsvRow> csvCache;
    public TopicMetadataService(TopicMetadataPromptBuilder promptBuilder,
                                AIProviderRouterService providerRouterService,
                                TopicMetadataProducer producer,
                                MessageService messageService,
                                PendingResponseSignalService pendingResponseSignalService,
                                ObjectMapper objectMapper,
                                TopicMetadataProperties properties) {
        this.promptBuilder = promptBuilder;
        this.providerRouterService = providerRouterService;
        this.producer = producer;
        this.messageService = messageService;
        this.pendingResponseSignalService = pendingResponseSignalService;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    public void processRefinedEvent(TopicClusterRefinedEvent event) {
        if (event == null || event.getClusters() == null || event.getClusters().isEmpty()) {
            return;
        }

        Workspace ws = new Workspace();
        ws.setId(event.getWorkspaceId());
        String tenantId = extractTenantId(event);
        if (!StringUtils.hasText(tenantId)) {
            logger.error("Step3: tenantId missing (set topic.metadata.tenant-id or include tenantId in event metadata)");
            producer.publishDlq(
                buildDlqPayload(event, null, "missing_tenant_id"),
                "step3_missing_tenant_id",
                event.getWorkspaceId(),
                event.getBatchId()
            );
            return;
        }
        ws.setTenantId(tenantId);
        ws.setDeemergeUserId("system");
        ws.setName(event.getWorkspaceId());

        AIProvider provider = providerRouterService.selectProvider(
            AITaskType.GENERATE_TOPIC,
            ws.getTenantId(),
            properties.getProviderHint()
        );
        if (provider == null) {
            logger.error("Step3: no AI provider available for workspace {}", event.getWorkspaceId());
            return;
        }

        for (TopicClusterRefined cluster : event.getClusters()) {
            try {
                TopicMetadataEvent metadataEvent = buildMetadataEvent(event, cluster, ws, provider);
                if (metadataEvent != null) {
                    producer.publish(metadataEvent);
                }
            } catch (Exception e) {
                producer.publishDlq(
                    buildDlqPayload(event, cluster, e.getMessage()),
                    "step3_processing_error:" + e.getMessage(),
                    event.getWorkspaceId(),
                    event.getBatchId()
                );
            }
        }
    }

    private TopicMetadataEvent buildMetadataEvent(TopicClusterRefinedEvent refinedEvent,
                                                 TopicClusterRefined cluster,
                                                 Workspace workspace,
                                                 AIProvider provider) {
        List<Message> messages = loadMessages(cluster.getMessageIds());
        String prompt = promptBuilder.buildPrompt(cluster, messages);
        String response = provider.processTextQuery(prompt, workspace.getDeemergeUserId(), workspace.getTenantId());
        JsonNode responseRoot = tryParseJson(response);

        logger.debug("🧾 Step3 raw LLM response ({} chars): {}",
            response != null ? response.length() : 0,
            response != null ? response.substring(0, Math.min(response.length(), 500)) + (response.length() > 500 ? "..." : "") : "null");

        TopicMetadata topic = parseMetadata(responseRoot);
        if (topic == null) {
            producer.publishDlq(
                buildDlqPayload(refinedEvent, cluster, "invalid_json_response"),
                "step3_invalid_json_response",
                refinedEvent.getWorkspaceId(),
                refinedEvent.getBatchId()
            );
            return null;
        }
        applyFallbacks(topic, cluster, messages);
        PendingResponseSignalService.PendingResponseSignal pendingSignal =
            pendingResponseSignalService.fromLlmNode(responseRoot).orElse(null);
        topic = pendingResponseSignalService.applyToTopic(topic, pendingSignal, null);
        topic = ensureNonEmptyActionItems(topic, cluster, provider, workspace);
        TopicMetadataEvent out = new TopicMetadataEvent();
        out.setEventId(UUID.randomUUID().toString());
        out.setCreatedAt(Instant.now());
        out.setWorkspaceId(refinedEvent.getWorkspaceId());
        out.setBatchId(refinedEvent.getBatchId());
        out.setClusterId(cluster.getClusterId());
        out.setTopicId(stableTopicId(refinedEvent.getWorkspaceId(), cluster.getClusterId()));
        out.setProviderId(provider.getProviderId());
        out.setPromptVersion(properties.getPromptVersion());
        out.setMessageIds(cluster.getMessageIds());
        out.setTopic(topic);
        Map<String, Object> metadata = new LinkedHashMap<>();
        if (refinedEvent.getMetadata() instanceof Map<?, ?> raw) {
            for (Map.Entry<?, ?> entry : raw.entrySet()) {
                if (entry.getKey() != null) {
                    metadata.put(String.valueOf(entry.getKey()), entry.getValue());
                }
            }
        }
        metadata.put("step", "3");
        metadata.put("promptVersion", properties.getPromptVersion());
        metadata.put("pendingResponseDetected", pendingSignal != null);
        if (pendingSignal != null) {
            metadata.put("pendingResponse", pendingResponseSignalService.toMetadataMap(pendingSignal));
        }
        out.setMetadata(metadata);
        return out;
    }

    private TopicMetadata ensureNonEmptyActionItems(TopicMetadata topic,
                                                   TopicClusterRefined cluster,
                                                   AIProvider provider,
                                                   Workspace workspace) {
        if (topic == null) {
            return null;
        }
        if (isUxComplete(topic)) {
            return topic;
        }

        if (properties.isRepairEmptyActionItemsEnabled()
            && properties.getRepairEmptyActionItemsMaxAttempts() > 0
            && provider != null
            && workspace != null) {
            for (int attempt = 1; attempt <= properties.getRepairEmptyActionItemsMaxAttempts(); attempt++) {
                try {
                    String repairPrompt = buildRepairPrompt(topic);
                    String repairedResponse = provider.processTextQuery(
                        repairPrompt,
                        workspace.getDeemergeUserId(),
                        workspace.getTenantId()
                    );
                    TopicMetadata repaired = parseMetadata(repairedResponse);
                    if (repaired != null) {
                        applyFallbacks(repaired, cluster, List.of());
                        if (isUxComplete(repaired)) {
                            return repaired;
                        }
                    }
                } catch (Exception e) {
                    logger.debug("Step3 repair attempt {} failed: {}", attempt, e.getMessage());
                }
            }
        }

        if (topic.getActionItems() == null || topic.getActionItems().isEmpty()) {
            TopicActionItem derived = deriveActionItem(topic);
            if (derived != null) {
                topic.setActionItems(List.of(derived));
            }
        }
        applyUxFallbacks(topic);
        return topic;
    }

    private boolean isUxComplete(TopicMetadata topic) {
        if (topic == null) {
            return false;
        }
        boolean hasActionItems = topic.getActionItems() != null && !topic.getActionItems().isEmpty();
        boolean hasSuggestedAction = StringUtils.hasText(topic.getSuggestedAction());
        boolean hasReason = StringUtils.hasText(topic.getReason());
        boolean hasSections = StringUtils.hasText(topic.getSituation())
            && StringUtils.hasText(topic.getImpact())
            && StringUtils.hasText(topic.getProposedSolution())
            && StringUtils.hasText(topic.getDecisionNeeded());
        boolean hasParticipants = topic.getParticipants() != null && !topic.getParticipants().isEmpty();
        boolean hasTags = topic.getTags() != null && !topic.getTags().isEmpty();
        return hasActionItems && hasSuggestedAction && hasReason && hasSections && hasParticipants && hasTags;
    }

    private void applyUxFallbacks(TopicMetadata topic) {
        if (topic == null) {
            return;
        }

        if (!StringUtils.hasText(topic.getSuggestedAction())) {
            String fromAction = null;
            if (topic.getActionItems() != null && !topic.getActionItems().isEmpty()) {
                TopicActionItem first = topic.getActionItems().get(0);
                fromAction = first != null ? first.getTask() : null;
            }
            if (!StringUtils.hasText(fromAction)) {
                fromAction = deriveTaskText(topic);
            }
            topic.setSuggestedAction(fromAction);
        }

        if (!StringUtils.hasText(topic.getReason())) {
            String reason = firstNonBlank(topic.getImpact(), topic.getDecisionNeeded(), topic.getSummary());
            if (StringUtils.hasText(reason)) {
                reason = reason.trim();
                int cut = reason.indexOf('.');
                reason = cut > 0 ? reason.substring(0, cut + 1).trim() : reason;
                topic.setReason(trimToMax(reason, 220));
            }
        }

        if (!StringUtils.hasText(topic.getSituation())) {
            topic.setSituation(firstNonBlank(topic.getSummary(), topic.getTitle()));
        }
        if (!StringUtils.hasText(topic.getImpact())) {
            topic.setImpact(firstNonBlank(topic.getReason(), topic.getSummary()));
        }
        if (!StringUtils.hasText(topic.getProposedSolution())) {
            topic.setProposedSolution(firstNonBlank(topic.getSuggestedAction(), topic.getSummary()));
        }
        if (!StringUtils.hasText(topic.getDecisionNeeded())) {
            topic.setDecisionNeeded(firstNonBlank(topic.getSuggestedAction(), topic.getProposedSolution()));
        }

        if (!StringUtils.hasText(topic.getPriority())) {
            topic.setPriority(StringUtils.hasText(topic.getUrgency()) ? topic.getUrgency().trim() : "medium");
        }
    }

    private String buildRepairPrompt(TopicMetadata topic) {
        Map<String, Object> canonical = new LinkedHashMap<>();
        canonical.put("title", topic.getTitle());
        canonical.put("summary", topic.getSummary());
        canonical.put("external_party", topic.getExternalParty());
        canonical.put("priority", topic.getPriority());
        canonical.put("due_date", topic.getDeadline());
        canonical.put("reason", topic.getReason());
        canonical.put("suggested_action", topic.getSuggestedAction());
        canonical.put("situation", topic.getSituation());
        canonical.put("impact", topic.getImpact());
        canonical.put("proposed_solution", topic.getProposedSolution());
        canonical.put("decision_needed", topic.getDecisionNeeded());
        canonical.put("participants", topic.getParticipants() != null ? topic.getParticipants() : List.of());
        canonical.put("action_items", List.of());
        canonical.put("urgency", topic.getUrgency());
        canonical.put("deadline", topic.getDeadline());
        canonical.put("status", topic.getStatus());
        canonical.put("channel", topic.getChannel());
        canonical.put("tags", topic.getTags() != null ? topic.getTags() : List.of());

        String json;
        try {
            json = objectMapper.writeValueAsString(canonical);
        } catch (Exception e) {
            json = "{}";
        }

        return """
You are fixing a JSON response. Return STRICT JSON ONLY (no markdown, no extra text).

Given the JSON below, rewrite it using the same schema but ensure:
- action_items is a non-empty list (at least 1 item)
- suggested_action is a non-empty string
- situation, impact, proposed_solution, decision_needed are non-empty strings (infer if missing)
- owner should be one of participants if possible; otherwise null
- due_date should be deadline if it exists; otherwise null
- keep other fields consistent with title/summary/channel

JSON:
%s
""".formatted(json);
    }

    private TopicActionItem deriveActionItem(TopicMetadata topic) {
        String task = deriveTaskText(topic);
        if (!StringUtils.hasText(task)) {
            return null;
        }
        TopicActionItem item = new TopicActionItem();
        item.setTask(task);
        if (topic.getParticipants() != null && !topic.getParticipants().isEmpty()) {
            item.setOwner(topic.getParticipants().get(0));
        }
        item.setDueDate(StringUtils.hasText(topic.getDeadline()) ? topic.getDeadline() : null);
        item.setStatus("pending");
        item.setPriority(StringUtils.hasText(topic.getUrgency()) ? topic.getUrgency() : "medium");
        return item;
    }

    private String deriveTaskText(TopicMetadata topic) {
        if (topic == null) {
            return null;
        }
        if (StringUtils.hasText(topic.getSuggestedAction())) {
            return trimToMax(topic.getSuggestedAction().trim(), 220);
        }
        if (StringUtils.hasText(topic.getDecisionNeeded())) {
            return trimToMax(topic.getDecisionNeeded().trim(), 220);
        }
        if (StringUtils.hasText(topic.getProposedSolution())) {
            return trimToMax(topic.getProposedSolution().trim(), 220);
        }
        if (StringUtils.hasText(topic.getSummary())) {
            String summary = topic.getSummary().trim();
            int cut = summary.indexOf('.');
            String first = cut > 0 ? summary.substring(0, cut + 1).trim() : summary;
            if (first.length() > 180) {
                first = first.substring(0, 180).trim();
            }
            return first;
        }
        if (StringUtils.hasText(topic.getTitle())) {
            return topic.getTitle().trim();
        }
        return null;
    }

    private String trimToMax(String value, int max) {
        if (!StringUtils.hasText(value)) {
            return value;
        }
        if (value.length() <= max) {
            return value;
        }
        return value.substring(0, max).trim();
    }

    private void applyFallbacks(TopicMetadata topic, TopicClusterRefined cluster, List<Message> messages) {
        if (topic == null) {
            return;
        }

        enrichActionItemOwnerIdentities(topic, messages);

        if (!StringUtils.hasText(topic.getPriority())) {
            if (StringUtils.hasText(topic.getUrgency())) {
                topic.setPriority(topic.getUrgency().trim());
            }
        }

        if (!StringUtils.hasText(topic.getSummary())) {
            String stitched = stitchSummary(topic);
            if (StringUtils.hasText(stitched)) {
                topic.setSummary(stitched);
            }
        }

        if (topic.getParticipants() == null || topic.getParticipants().isEmpty()) {
            LinkedHashSet<String> participants = new LinkedHashSet<>();

            if (cluster != null && cluster.getParticipants() != null) {
                cluster.getParticipants().stream()
                    .filter(StringUtils::hasText)
                    .map(String::trim)
                    .forEach(participants::add);
            }

            if (topic.getActionItems() != null) {
                for (TopicActionItem item : topic.getActionItems()) {
                    if (item == null || !StringUtils.hasText(item.getOwner())) {
                        continue;
                    }
                    String owner = item.getOwner().trim();
                    if (owner.startsWith("@")) {
                        owner = owner.substring(1);
                    }
                    if (StringUtils.hasText(owner)) {
                        participants.add(owner);
                    }
                }
            }

            if (messages != null) {
                for (Message m : messages) {
                    if (m == null) continue;
                    String user = StringUtils.hasText(m.getDisplayName()) ? m.getDisplayName()
                        : StringUtils.hasText(m.getUsername()) ? m.getUsername() : null;
                    if (StringUtils.hasText(user)) {
                        participants.add(user.trim());
                    }
                }
            }

            if (!participants.isEmpty()) {
                topic.setParticipants(new ArrayList<>(participants));
                logger.debug("Step3 fallback participants applied: {}", topic.getParticipants());
            }
        }

        if (topic.getTags() == null || topic.getTags().isEmpty()) {
            List<String> tags = deriveTags(topic, cluster);
            if (!tags.isEmpty()) {
                topic.setTags(tags);
                logger.debug("Step3 fallback tags applied: {}", topic.getTags());
            }
        }
    }

    private String stitchSummary(TopicMetadata topic) {
        if (topic == null) {
            return null;
        }
        List<String> parts = new ArrayList<>();
        if (StringUtils.hasText(topic.getSituation())) parts.add(topic.getSituation().trim());
        if (StringUtils.hasText(topic.getImpact())) parts.add(topic.getImpact().trim());
        if (StringUtils.hasText(topic.getDecisionNeeded())) parts.add(topic.getDecisionNeeded().trim());
        if (parts.isEmpty()) {
            return null;
        }
        String joined = String.join(" ", parts);
        return trimToMax(joined, 500);
    }

    private void enrichActionItemOwnerIdentities(TopicMetadata topic, List<Message> messages) {
        if (topic == null || topic.getActionItems() == null || topic.getActionItems().isEmpty()) {
            return;
        }
        if (messages == null || messages.isEmpty()) {
            return;
        }

        Map<String, OwnerIdentity> identityByKey = new HashMap<>();
        for (Message m : messages) {
            if (m == null) continue;
            String display = firstNonBlank(m.getDisplayName(), m.getUsername(), m.getName());
            String userId = firstNonBlank(m.getSlackUserId(), m.getUniqueUserId(), m.getUserId());
            String email = m.getEmail();
            if (!StringUtils.hasText(display) && !StringUtils.hasText(userId) && !StringUtils.hasText(email)) {
                continue;
            }
            OwnerIdentity id = new OwnerIdentity(display, userId, email);
            if (StringUtils.hasText(display)) {
                identityByKey.putIfAbsent(normalizeIdentityKey(display), id);
            }
            if (StringUtils.hasText(m.getUsername())) {
                identityByKey.putIfAbsent(normalizeIdentityKey(m.getUsername()), id);
            }
            if (StringUtils.hasText(email)) {
                identityByKey.putIfAbsent(normalizeIdentityKey(email), id);
            }
            if (StringUtils.hasText(userId)) {
                identityByKey.putIfAbsent(normalizeIdentityKey(userId), id);
            }
        }

        for (TopicActionItem ai : topic.getActionItems()) {
            if (ai == null) continue;
            if (StringUtils.hasText(ai.getOwnerUserId()) || StringUtils.hasText(ai.getOwnerEmail())) {
                continue;
            }
            String owner = ai.getOwner();
            if (!StringUtils.hasText(owner)) {
                continue;
            }
            OwnerIdentity resolved = identityByKey.get(normalizeIdentityKey(owner));
            if (resolved == null) {
                continue;
            }
            if (StringUtils.hasText(resolved.userId)) {
                ai.setOwnerUserId(resolved.userId);
            }
            if (StringUtils.hasText(resolved.email)) {
                ai.setOwnerEmail(resolved.email);
            }
            if (!StringUtils.hasText(ai.getOwner()) && StringUtils.hasText(resolved.displayName)) {
                ai.setOwner(resolved.displayName);
            }
        }
    }

    private String normalizeIdentityKey(String raw) {
        if (!StringUtils.hasText(raw)) {
            return "";
        }
        String s = raw.trim();
        if (s.startsWith("@")) {
            s = s.substring(1).trim();
        }
        return s.toLowerCase();
    }

    private String firstNonBlank(String... values) {
        if (values == null) return null;
        for (String v : values) {
            if (StringUtils.hasText(v)) return v;
        }
        return null;
    }

    private record OwnerIdentity(String displayName, String userId, String email) {}

    private List<String> deriveTags(TopicMetadata topic, TopicClusterRefined cluster) {
        LinkedHashSet<String> tags = new LinkedHashSet<>();

        if (StringUtils.hasText(topic.getExternalParty())) {
            tags.add(topic.getExternalParty().trim());
        }

        if (cluster != null && StringUtils.hasText(cluster.getChannel())) {
            String channel = cluster.getChannel().trim();
            if (channel.startsWith("#")) {
                channel = channel.substring(1);
            }
            if (StringUtils.hasText(channel)) {
                tags.add(channel);
            }
        } else if (StringUtils.hasText(topic.getChannel())) {
            String channel = topic.getChannel().trim();
            if (channel.startsWith("#")) {
                channel = channel.substring(1);
            }
            if (StringUtils.hasText(channel)) {
                tags.add(channel);
            }
        }

        if (StringUtils.hasText(topic.getTitle())) {
            String normalized = topic.getTitle().toLowerCase().replaceAll("[^a-z0-9 ]", " ");
            for (String raw : normalized.split("\\s+")) {
                if (!StringUtils.hasText(raw) || raw.length() < 3) continue;
                tags.add(raw);
                if (tags.size() >= Math.max(5, properties.getMinTagsFallback())) {
                    // keep collecting a bit more but cap size
                    if (tags.size() >= properties.getMaxTagsFallback()) break;
                }
            }
        }

        return tags.stream().filter(StringUtils::hasText).limit(properties.getMaxTagsFallback()).toList();
    }

    private List<Message> loadMessages(List<String> messageIds) {
        if (messageIds == null || messageIds.isEmpty()) {
            return List.of();
        }
        List<Message> out = new ArrayList<>();
        int limit = Math.min(messageIds.size(), properties.getMaxMessages());
        for (int i = 0; i < limit; i++) {
            String id = messageIds.get(i);
            if (!StringUtils.hasText(id)) continue;
            Optional<Message> message = messageService.findById(id);
            message.ifPresent(out::add);
            if (message.isEmpty() && properties.isCsvFallbackEnabled()) {
                Message fromCsv = lookupCsvMessage(id);
                if (fromCsv != null) {
                    out.add(fromCsv);
                }
            }
        }
        return out;
    }

    private Message lookupCsvMessage(String messageId) {
        if (!StringUtils.hasText(properties.getCsvPath())) {
            return null;
        }
        String trimmed = messageId.trim();
        if (!trimmed.chars().allMatch(Character::isDigit)) {
            return null;
        }
        int id;
        try {
            id = Integer.parseInt(trimmed);
        } catch (Exception e) {
            return null;
        }
        CsvRow row = getCsvCache().get(id);
        if (row == null) {
            return null;
        }
        Message m = new Message();
        m.setId(trimmed);
        m.setChannelName(row.channel);
        m.setChannelId(row.channel);
        m.setDisplayName(row.userName);
        m.setUsername(row.userName);
        m.setUserId(row.userId);
        m.setSlackUserId(row.userId);
        m.setUniqueUserId(row.userId);
        m.setText(row.text);
        m.setThreadTs(row.threadId);
        m.setMessageTs(row.timestamp);
        return m;
    }

    private Map<Integer, CsvRow> getCsvCache() {
        Map<Integer, CsvRow> local = csvCache;
        if (local != null) {
            return local;
        }
        synchronized (this) {
            if (csvCache != null) {
                return csvCache;
            }
            csvCache = loadCsv(Path.of(properties.getCsvPath()));
            return csvCache;
        }
    }

    private Map<Integer, CsvRow> loadCsv(Path path) {
        Map<Integer, CsvRow> map = new HashMap<>();
        try {
            if (!Files.exists(path)) {
                logger.warn("Step3 CSV fallback enabled but file not found: {}", path);
                return map;
            }
            List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
            if (lines.isEmpty()) {
                return map;
            }
            for (int i = 1; i < lines.size(); i++) {
                String line = lines.get(i);
                String[] cols = CSV_SPLIT.split(line, -1);
                if (cols.length < 5) continue;
                // columns: channel,user_name,user_id,timestamp,text,thread_id
                CsvRow row = new CsvRow(
                    stripQuotes(cols[0]),
                    stripQuotes(cols[1]),
                    stripQuotes(cols[2]),
                    stripQuotes(cols[3]),
                    stripQuotes(cols[4]),
                    cols.length > 5 ? nullIfNone(stripQuotes(cols[5])) : null
                );
                map.put(i, row);
            }
            logger.info("Step3 CSV fallback loaded {} messages from {}", map.size(), path);
        } catch (Exception e) {
            logger.warn("Step3 CSV fallback failed to load {}: {}", path, e.getMessage());
        }
        return map;
    }

    private String stripQuotes(String v) {
        if (v == null) return "";
        String s = v.trim();
        if (s.length() >= 2 && s.startsWith("\"") && s.endsWith("\"")) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    private String nullIfNone(String val) {
        if (!StringUtils.hasText(val)) {
            return null;
        }
        return "None".equalsIgnoreCase(val) ? null : val;
    }

    private TopicMetadata parseMetadata(String response) {
        JsonNode root = tryParseJson(response);
        return parseMetadata(root);
    }

    private TopicMetadata parseMetadata(JsonNode root) {
        if (root == null || !root.isObject()) {
            return null;
        }

        TopicMetadata metadata = new TopicMetadata();
        metadata.setTitle(text(root, "title"));
        metadata.setSummary(text(root, "summary"));
        metadata.setExternalParty(text(root, "external_party", "externalParty", "external_party_name", "externalPartyName"));
        metadata.setPriority(text(root, "priority", "priority_level", "priorityLevel"));
        metadata.setReason(text(root, "reason", "due_reason", "dueReason"));
        metadata.setSuggestedAction(text(root, "suggested_action", "suggestedAction", "suggested_action_text", "suggestedActionText"));
        metadata.setSituation(text(root, "situation"));
        metadata.setImpact(text(root, "impact"));
        metadata.setProposedSolution(text(root, "proposed_solution", "proposedSolution"));
        metadata.setDecisionNeeded(text(root, "decision_needed", "decisionNeeded"));
        metadata.setChannel(text(root, "channel"));
        metadata.setUrgency(text(root, "urgency"));
        metadata.setDeadline(text(root, "deadline", "due_date", "dueDate"));
        metadata.setStatus(text(root, "status"));
        metadata.setTags(asTextList(root.get("tags")));
        metadata.setParticipants(asTextList(root.get("participants")));

        JsonNode actionItems = root.get("action_items");
        if (actionItems == null) {
            actionItems = root.get("actionItems");
        }
        if (actionItems != null && actionItems.isArray()) {
            List<TopicActionItem> items = new ArrayList<>();
            for (JsonNode item : actionItems) {
                if (!item.isObject()) continue;
                TopicActionItem ai = new TopicActionItem();
                ai.setTask(text(item, "task"));
                JsonNode ownerNode = item.get("owner");
                if (ownerNode != null && ownerNode.isObject()) {
                    ai.setOwner(text(ownerNode, "display_name", "displayName", "name", "owner"));
                    ai.setOwnerUserId(text(ownerNode, "user_id", "userId", "slack_user_id", "slackUserId"));
                    ai.setOwnerEmail(text(ownerNode, "email"));
                } else {
                    ai.setOwner(text(item, "owner"));
                    ai.setOwnerUserId(text(item, "owner_user_id", "ownerUserId", "slack_user_id", "slackUserId"));
                    ai.setOwnerEmail(text(item, "owner_email", "ownerEmail", "email"));
                }
                ai.setDueDate(text(item, "due_date", "dueDate"));
                ai.setStatus(text(item, "status"));
                ai.setPriority(text(item, "priority"));
                items.add(ai);
            }
            metadata.setActionItems(items);
        }
        return metadata;
    }

    private JsonNode tryParseJson(String response) {
        if (!StringUtils.hasText(response)) {
            return null;
        }
        String normalized = normalizeResponse(response);
        try {
            ObjectMapper lenientMapper = objectMapper.copy()
                .enable(JsonParser.Feature.ALLOW_COMMENTS)
                .enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES)
                .enable(JsonReadFeature.ALLOW_TRAILING_COMMA.mappedFeature())
                .enable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature());
            return lenientMapper.readTree(normalized);
        } catch (Exception e) {
            int start = normalized.indexOf('{');
            int end = normalized.lastIndexOf('}');
            if (start >= 0 && end > start) {
                try {
                    String sliced = normalized.substring(start, end + 1);
                    ObjectMapper lenientMapper = objectMapper.copy()
                        .enable(JsonParser.Feature.ALLOW_COMMENTS)
                        .enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES)
                        .enable(JsonReadFeature.ALLOW_TRAILING_COMMA.mappedFeature())
                        .enable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature());
                    return lenientMapper.readTree(sliced);
                } catch (Exception ignored) {
                    logger.debug("Step3: failed to parse sliced JSON block: {}", ignored.getMessage());
                }
            }
            logger.warn("Step3: unable to parse LLM response as JSON; len={} chars", response.length());
            return null;
        }
    }

    private String normalizeResponse(String response) {
        String trimmed = response.trim();
        trimmed = trimmed.replaceAll("```json", "")
            .replaceAll("```", "")
            .trim();
        trimmed = trimmed.replaceAll(",\\s*}", "}")
            .replaceAll(",\\s*]", "]");
        return trimmed;
    }

    private List<String> asTextList(JsonNode node) {
        if (node == null) {
            return List.of();
        }
        if (node.isArray()) {
            List<String> values = new ArrayList<>();
            for (JsonNode child : node) {
                if (child == null || child.isNull()) {
                    continue;
                }
                if (child.isTextual() && StringUtils.hasText(child.asText())) {
                    values.add(child.asText().trim());
                } else if (child.isNumber()) {
                    values.add(child.asText());
                } else if (child.isObject()) {
                    JsonNode name = child.get("name");
                    if (name != null && name.isTextual() && StringUtils.hasText(name.asText())) {
                        values.add(name.asText().trim());
                    }
                }
            }
            return values.stream().filter(StringUtils::hasText).distinct().toList();
        }
        if (node.isTextual()) {
            String raw = node.asText();
            if (!StringUtils.hasText(raw)) {
                return List.of();
            }
            return java.util.Arrays.stream(CSV_SPLIT.split(raw, -1))
                .map(String::trim)
                .filter(StringUtils::hasText)
                .toList();
        }
        return List.of();
    }

    private String text(JsonNode node, String... keys) {
        if (node == null) {
            return null;
        }
        for (String key : keys) {
            JsonNode v = node.get(key);
            if (v != null && v.isTextual()) {
                return v.asText();
            }
        }
        return null;
    }

    private String stableTopicId(String workspaceId, String clusterId) {
        String basis = (workspaceId == null ? "" : workspaceId) + "|" + (clusterId == null ? "" : clusterId);
        return UUID.nameUUIDFromBytes(basis.getBytes(StandardCharsets.UTF_8)).toString();
    }

    private String safeJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            return String.valueOf(obj);
        }
    }

    private String buildDlqPayload(TopicClusterRefinedEvent event, TopicClusterRefined cluster, String reason) {
        try {
            Map<String, Object> payload = new HashMap<>();
            payload.put("reason", reason);
            payload.put("workspaceId", event != null ? event.getWorkspaceId() : null);
            payload.put("batchId", event != null ? event.getBatchId() : null);
            payload.put("clusterId", cluster != null ? cluster.getClusterId() : null);
            payload.put("messageIds", cluster != null ? cluster.getMessageIds() : null);
            return objectMapper.writeValueAsString(payload);
        } catch (Exception e) {
            return String.valueOf(event);
        }
    }

    private String extractTenantId(TopicClusterRefinedEvent event) {
        Object metadata = event.getMetadata();
        if (metadata instanceof Map<?, ?> map) {
            Object tenantId = map.get("tenantId");
            if (tenantId instanceof String s && StringUtils.hasText(s)) {
                return s;
            }
        }
        if (StringUtils.hasText(properties.getTenantId())) {
            return properties.getTenantId();
        }
        return null;
    }

    private record CsvRow(String channel, String userName, String userId, String timestamp, String text, String threadId) {}
}
