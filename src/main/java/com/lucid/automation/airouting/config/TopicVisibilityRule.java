package com.lucid.automation.airouting.config;

import org.springframework.util.StringUtils;

public enum TopicVisibilityRule {
    OWNS_OPEN_ACTION_ITEM,
    OWNS_OPEN_ACTION_ITEM_AND_CHANNEL;

    public static TopicVisibilityRule from(String raw) {
        if (!StringUtils.hasText(raw)) {
            return OWNS_OPEN_ACTION_ITEM_AND_CHANNEL;
        }
        String v = raw.trim().toLowerCase();
        return switch (v) {
            case "owns_open_action_item", "owns_open_action_item_only", "action_item_owner" -> OWNS_OPEN_ACTION_ITEM;
            case "owns_open_action_item_and_member_of_channel", "owns_open_action_item_and_channel", "owner_and_channel" ->
                OWNS_OPEN_ACTION_ITEM_AND_CHANNEL;
            default -> OWNS_OPEN_ACTION_ITEM_AND_CHANNEL;
        };
    }
}

