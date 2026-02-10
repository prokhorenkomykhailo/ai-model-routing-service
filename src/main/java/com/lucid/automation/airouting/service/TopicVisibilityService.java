package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.config.TopicVisibilityProperties;
import com.lucid.automation.airouting.config.TopicVisibilityRule;
import com.lucid.automation.airouting.dto.topic.TopicActionItem;
import com.lucid.automation.airouting.dto.topic.TopicMetadata;
import com.lucid.automation.airouting.dto.topic.TopicMetadataEvent;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
public class TopicVisibilityService {

    private final ChannelMembershipService channelMembershipService;
    private final TopicVisibilityProperties properties;

    public TopicVisibilityService(ChannelMembershipService channelMembershipService, TopicVisibilityProperties properties) {
        this.channelMembershipService = channelMembershipService;
        this.properties = properties;
    }

    public List<UserTopicVisibility> computeVisibility(TopicMetadataEvent event) {
        return evaluate(event).matches();
    }

    public VisibilityEvaluation evaluate(TopicMetadataEvent event) {
        if (event == null || event.getTopic() == null || !StringUtils.hasText(event.getTopicId())) {
            return new VisibilityEvaluation(null, null, null, null, Set.of(), List.of(), VisibilitySkipReason.INVALID_EVENT);
        }

        String workspaceId = event.getWorkspaceId();
        String batchId = event.getBatchId();
        TopicMetadata topic = event.getTopic();
        String channel = topic.getChannel();

        Set<String> owners = extractOpenOwners(topic.getActionItems());
        if (owners.isEmpty()) {
            return new VisibilityEvaluation(workspaceId, batchId, event.getTopicId(), channel, Set.of(), List.of(),
                VisibilitySkipReason.NO_OPEN_ACTION_ITEM_OWNERS);
        }

        List<UserTopicVisibility> out = new ArrayList<>();

        TopicVisibilityRule rule = TopicVisibilityRule.from(properties.getRule());
        Set<String> candidateOwnerKeys = new LinkedHashSet<>(owners);

        for (String ownerKey : owners) {
            String resolvedUserKey;
            if (rule == TopicVisibilityRule.OWNS_OPEN_ACTION_ITEM_AND_CHANNEL) {
                resolvedUserKey = channelMembershipService.resolveBestMemberKey(workspaceId, ownerKey, channel);
            } else {
                resolvedUserKey = channelMembershipService.canonicalize(ownerKey);
            }
            if (!StringUtils.hasText(resolvedUserKey)) {
                continue;
            }
            out.add(new UserTopicVisibility(workspaceId, resolvedUserKey, event.getTopicId(), batchId, channel));
        }

        VisibilitySkipReason reason = out.isEmpty()
            ? (rule == TopicVisibilityRule.OWNS_OPEN_ACTION_ITEM_AND_CHANNEL
                ? VisibilitySkipReason.NO_CHANNEL_MEMBERSHIP_MATCHES
                : VisibilitySkipReason.NO_MATCHES)
            : VisibilitySkipReason.OK;

        return new VisibilityEvaluation(workspaceId, batchId, event.getTopicId(), channel, candidateOwnerKeys, out, reason);
    }

    private Set<String> extractOpenOwners(List<TopicActionItem> items) {
        if (items == null || items.isEmpty()) {
            return Set.of();
        }
        Set<String> owners = new LinkedHashSet<>();
        for (TopicActionItem ai : items) {
            if (ai == null) {
                continue;
            }
            if (!isOpen(ai.getStatus())) {
                continue;
            }
            String ownerKey = null;
            if (StringUtils.hasText(ai.getOwnerUserId())) {
                ownerKey = ai.getOwnerUserId();
            } else if (StringUtils.hasText(ai.getOwnerEmail())) {
                ownerKey = ai.getOwnerEmail();
            } else if (StringUtils.hasText(ai.getOwner())) {
                ownerKey = ai.getOwner();
            }
            if (StringUtils.hasText(ownerKey)) {
                owners.add(ownerKey.trim());
            }
        }
        return owners;
    }

    private boolean isOpen(String status) {
        if (!StringUtils.hasText(status)) {
            return true;
        }
        String s = status.trim().toLowerCase();
        return !(s.equals("done") || s.equals("closed") || s.equals("completed") || s.equals("resolved"));
    }

    public record UserTopicVisibility(String workspaceId, String userId, String topicId, String batchId, String channel) {}

    public enum VisibilitySkipReason {
        OK,
        INVALID_EVENT,
        NO_OPEN_ACTION_ITEM_OWNERS,
        NO_CHANNEL_MEMBERSHIP_MATCHES,
        NO_MATCHES
    }

    public record VisibilityEvaluation(
        String workspaceId,
        String batchId,
        String topicId,
        String channel,
        Set<String> candidateOwnerKeys,
        List<UserTopicVisibility> matches,
        VisibilitySkipReason skipReason
    ) {}
}
