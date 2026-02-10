package com.lucid.automation.airouting.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucid.automation.airouting.config.TopicVisibilityProperties;
import com.lucid.automation.common.dto.event.UserChannelEventDTO;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
public class ChannelMembershipService {

    private static final Pattern EMAIL = Pattern.compile("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$");

    private final TopicVisibilityProperties properties;
    private final ObjectMapper objectMapper;

    private final ConcurrentHashMap<String, Set<String>> userChannelsByWorkspace = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> identityAliases = new ConcurrentHashMap<>();

    public ChannelMembershipService(TopicVisibilityProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
        seedFromConfig();
    }

    public boolean isMember(String workspaceId, String userId, String channelName) {
        if (!StringUtils.hasText(channelName)) {
            return true;
        }
        if (!StringUtils.hasText(workspaceId) || !StringUtils.hasText(userId)) {
            return !properties.isStrictChannelMembership();
        }

        Set<String> channels = userChannelsByWorkspace.get(key(workspaceId, userId));
        if (channels == null || channels.isEmpty()) {
            return !properties.isStrictChannelMembership();
        }
        return channels.contains(channelName);
    }

    public boolean isMemberAny(String workspaceId, String ownerKey, String channelName) {
        for (String candidate : resolveIdentityCandidates(ownerKey)) {
            if (isMember(workspaceId, candidate, channelName)) {
                return true;
            }
        }
        return !properties.isStrictChannelMembership();
    }

    public String canonicalize(String rawOwnerKey) {
        List<String> candidates = resolveIdentityCandidates(rawOwnerKey);
        return candidates.isEmpty() ? null : candidates.get(0);
    }

    /**
     * Returns a best-effort user key to emit for visibility events. If strict channel membership is enabled,
     * this returns null when no candidate is a member of the channel.
     */
    public String resolveBestMemberKey(String workspaceId, String rawOwnerKey, String channelName) {
        List<String> candidates = resolveIdentityCandidates(rawOwnerKey);
        if (candidates.isEmpty()) {
            return null;
        }

        for (String candidate : candidates) {
            if (isMember(workspaceId, candidate, channelName)) {
                return candidate;
            }
        }

        return properties.isStrictChannelMembership() ? null : candidates.get(0);
    }

    public List<String> resolveIdentityCandidates(String rawOwnerKey) {
        if (!StringUtils.hasText(rawOwnerKey)) {
            return List.of();
        }
        String owner = rawOwnerKey.trim();
        if (owner.startsWith("@")) {
            owner = owner.substring(1).trim();
        }
        if (!StringUtils.hasText(owner)) {
            return List.of();
        }

        List<String> out = new ArrayList<>();

        String aliasTarget = identityAliases.get(owner);
        if (StringUtils.hasText(aliasTarget)) {
            out.add(aliasTarget.trim());
        }

        if (EMAIL.matcher(owner).matches()) {
            out.add(owner.toLowerCase());
        }

        out.add(owner);
        return out.stream().filter(StringUtils::hasText).distinct().toList();
    }

    public void upsertFromUserChannelEvent(UserChannelEventDTO event) {
        if (event == null) {
            return;
        }
        String workspaceId = StringUtils.hasText(event.getTeamId()) ? event.getTeamId() : event.getTeamName();
        String channelName = event.getChannelName();
        if (!StringUtils.hasText(workspaceId) || !StringUtils.hasText(channelName)) {
            return;
        }
        if (event.getSlackUsers() == null || event.getSlackUsers().isEmpty()) {
            return;
        }

        for (UserChannelEventDTO.SlackUserInfo info : event.getSlackUsers()) {
            if (info == null) {
                continue;
            }
            if (StringUtils.hasText(info.getSlackUserId())) {
                userChannelsByWorkspace.computeIfAbsent(key(workspaceId, info.getSlackUserId()),
                        k -> ConcurrentHashMap.newKeySet())
                    .add(channelName);
            }
            if (StringUtils.hasText(info.getSlackUserName())) {
                userChannelsByWorkspace.computeIfAbsent(key(workspaceId, info.getSlackUserName()),
                        k -> ConcurrentHashMap.newKeySet())
                    .add(channelName);
            }
        }
    }

    public Map<String, Set<String>> snapshot(String workspaceId) {
        if (!StringUtils.hasText(workspaceId)) {
            return Collections.emptyMap();
        }
        Map<String, Set<String>> out = new java.util.HashMap<>();
        for (Map.Entry<String, Set<String>> e : userChannelsByWorkspace.entrySet()) {
            String k = e.getKey();
            if (!k.startsWith(workspaceId + ":")) {
                continue;
            }
            out.put(k.substring((workspaceId + ":").length()), Set.copyOf(e.getValue()));
        }
        return out;
    }

    private void seedFromConfig() {
        Map<String, java.util.List<String>> map = properties.parseUserChannels(objectMapper);
        if (map.isEmpty()) {
            // still load identity aliases if present
            identityAliases.putAll(properties.parseIdentityAliases(objectMapper));
            return;
        }
        // config is userId -> channels, assumed single workspace for local runs
        String defaultWorkspace = "ws-gemini";
        for (Map.Entry<String, java.util.List<String>> e : map.entrySet()) {
            String userId = e.getKey();
            if (!StringUtils.hasText(userId)) {
                continue;
            }
            Set<String> set = ConcurrentHashMap.newKeySet();
            for (String c : e.getValue()) {
                if (StringUtils.hasText(c)) {
                    set.add(c.trim());
                }
            }
            if (!set.isEmpty()) {
                userChannelsByWorkspace.put(key(defaultWorkspace, userId), set);
            }
        }

        identityAliases.putAll(properties.parseIdentityAliases(objectMapper));
    }

    private String key(String workspaceId, String userId) {
        return workspaceId + ":" + userId;
    }
}
