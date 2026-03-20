package com.lucid.automation.airouting.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.lucid.automation.common.dto.topic.TopicActionItem;
import com.lucid.automation.common.dto.topic.TopicMetadata;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
public class PendingResponseSignalService {

    private static final Set<String> CLOSED_ACTION_STATUSES = Set.of("done", "closed", "completed", "resolved");

    public record PendingResponseSignal(
        String type,
        String requester,
        String requesterUserId,
        String assignee,
        String assigneeUserId,
        String requestText,
        String reason,
        String suggestedAction,
        String evidence
    ) {
        public boolean waitingOnCurrentUser() {
            return "waiting_on_current_user".equals(type);
        }
    }

    public Optional<PendingResponseSignal> fromLlmNode(JsonNode root) {
        if (root == null || root.isNull()) {
            return Optional.empty();
        }
        JsonNode signalNode = root.path("pending_response");
        if (signalNode.isMissingNode() || signalNode.isNull()) {
            signalNode = root.path("pendingResponse");
        }
        if ((signalNode.isMissingNode() || signalNode.isNull()) && root.has("metadata") && root.get("metadata").isObject()) {
            JsonNode metadata = root.get("metadata");
            signalNode = metadata.path("pending_response");
            if (signalNode.isMissingNode() || signalNode.isNull()) {
                signalNode = metadata.path("pendingResponse");
            }
        }
        if (signalNode.isMissingNode() || signalNode.isNull() || !signalNode.isObject()) {
            return Optional.empty();
        }

        boolean detected = bool(signalNode, "detected");
        if (!detected) {
            detected = hasAnyValue(
                text(signalNode, "type"),
                text(signalNode, "request_text", "requestText"),
                text(signalNode, "suggested_action", "suggestedAction")
            );
        }
        if (!detected) {
            return Optional.empty();
        }

        String type = firstNonBlank(
            text(signalNode, "type"),
            "waiting_on_other_party"
        );
        String requester = text(signalNode, "requester");
        String requesterUserId = text(signalNode, "requester_user_id", "requesterUserId");
        String assignee = text(signalNode, "assignee");
        String assigneeUserId = text(signalNode, "assignee_user_id", "assigneeUserId");
        String requestText = text(signalNode, "request_text", "requestText");
        String reason = text(signalNode, "reason");
        String suggestedAction = text(signalNode, "suggested_action", "suggestedAction");
        String evidence = text(signalNode, "evidence");

        if (!StringUtils.hasText(reason)) {
            reason = "Pending response detected by LLM.";
        }
        if (!StringUtils.hasText(suggestedAction)) {
            suggestedAction = "Follow up on the pending request.";
        }

        return Optional.of(new PendingResponseSignal(
            type,
            requester,
            requesterUserId,
            assignee,
            assigneeUserId,
            requestText,
            reason,
            suggestedAction,
            evidence
        ));
    }

    public TopicMetadata applyToTopic(TopicMetadata topic, PendingResponseSignal signal, String fallbackOwner) {
        if (topic == null || signal == null) {
            return topic;
        }

        if (!StringUtils.hasText(topic.getReason())) {
            topic.setReason(signal.reason());
        }
        if (!StringUtils.hasText(topic.getSuggestedAction())) {
            topic.setSuggestedAction(signal.suggestedAction());
        }
        if (!StringUtils.hasText(topic.getStatus())) {
            topic.setStatus("pending");
        }
        if (!StringUtils.hasText(topic.getUrgency())) {
            topic.setUrgency(signal.waitingOnCurrentUser() ? "high" : "medium");
        }
        if (!StringUtils.hasText(topic.getPriority())) {
            topic.setPriority(topic.getUrgency());
        }

        LinkedHashSet<String> participants = new LinkedHashSet<>();
        if (topic.getParticipants() != null) {
            participants.addAll(topic.getParticipants().stream().filter(StringUtils::hasText).map(String::trim).toList());
        }
        if (StringUtils.hasText(signal.requester())) {
            participants.add(signal.requester().trim());
        }
        if (StringUtils.hasText(signal.assignee()) && !"other party".equalsIgnoreCase(signal.assignee())) {
            participants.add(signal.assignee().trim());
        }
        topic.setParticipants(new ArrayList<>(participants));

        List<TopicActionItem> actionItems = topic.getActionItems() == null
            ? new ArrayList<>()
            : new ArrayList<>(topic.getActionItems());
        if (!hasOpenActionForSignal(actionItems, signal)) {
            TopicActionItem ai = new TopicActionItem();
            ai.setTask(signal.suggestedAction());
            String owner = signal.waitingOnCurrentUser() ? signal.assignee() : signal.requester();
            if (!StringUtils.hasText(owner)) {
                owner = fallbackOwner;
            }
            ai.setOwner(owner);
            ai.setOwnerUserId(signal.waitingOnCurrentUser() ? signal.assigneeUserId() : signal.requesterUserId());
            ai.setStatus("pending");
            ai.setPriority(signal.waitingOnCurrentUser() ? "high" : "medium");
            actionItems.add(ai);
        }
        topic.setActionItems(actionItems);

        LinkedHashSet<String> tags = new LinkedHashSet<>();
        if (topic.getTags() != null) {
            tags.addAll(topic.getTags().stream().filter(StringUtils::hasText).map(String::trim).toList());
        }
        tags.add("pending-response");
        tags.add(signal.waitingOnCurrentUser() ? "awaiting-user-response" : "awaiting-external-response");
        topic.setTags(new ArrayList<>(tags));

        return topic;
    }

    public Map<String, Object> toMetadataMap(PendingResponseSignal signal) {
        if (signal == null) {
            return Map.of("detected", false);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("detected", true);
        out.put("type", signal.type());
        out.put("requester", signal.requester());
        out.put("requesterUserId", signal.requesterUserId());
        out.put("assignee", signal.assignee());
        out.put("assigneeUserId", signal.assigneeUserId());
        out.put("requestText", signal.requestText());
        out.put("reason", signal.reason());
        out.put("suggestedAction", signal.suggestedAction());
        out.put("evidence", signal.evidence());
        return out;
    }

    private boolean hasOpenActionForSignal(List<TopicActionItem> items, PendingResponseSignal signal) {
        if (items == null || items.isEmpty()) {
            return false;
        }
        String normalized = normalize(signal.suggestedAction());
        for (TopicActionItem item : items) {
            if (item == null || !StringUtils.hasText(item.getTask())) {
                continue;
            }
            String status = StringUtils.hasText(item.getStatus()) ? item.getStatus().trim().toLowerCase(Locale.ROOT) : "";
            if (CLOSED_ACTION_STATUSES.contains(status)) {
                continue;
            }
            if (normalize(item.getTask()).contains(normalized) || normalized.contains(normalize(item.getTask()))) {
                return true;
            }
        }
        return false;
    }

    private boolean bool(JsonNode node, String key) {
        JsonNode v = node.get(key);
        if (v == null || v.isNull()) {
            return false;
        }
        if (v.isBoolean()) {
            return v.asBoolean();
        }
        if (v.isTextual()) {
            return Boolean.parseBoolean(v.asText().trim());
        }
        return false;
    }

    private String text(JsonNode node, String... keys) {
        if (node == null || keys == null) {
            return null;
        }
        for (String key : keys) {
            JsonNode v = node.get(key);
            if (v == null || v.isNull()) {
                continue;
            }
            if (v.isTextual() && StringUtils.hasText(v.asText())) {
                return v.asText().trim();
            }
        }
        return null;
    }

    private boolean hasAnyValue(String... values) {
        if (values == null) {
            return false;
        }
        for (String value : values) {
            if (StringUtils.hasText(value)) {
                return true;
            }
        }
        return false;
    }

    private String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (StringUtils.hasText(value)) {
                return value.trim();
            }
        }
        return null;
    }

    private String normalize(String value) {
        if (!StringUtils.hasText(value)) {
            return "";
        }
        return value.trim().toLowerCase(Locale.ROOT);
    }
}
