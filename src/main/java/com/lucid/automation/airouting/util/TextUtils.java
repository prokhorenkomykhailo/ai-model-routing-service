package com.lucid.automation.airouting.util;

import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.lucid.automation.airouting.dto.UserDTO;

public class TextUtils {
    /**
     * Replaces Slack mentions (e.g., <@U12345>) in the input text with the display name or username from userInfos.
     * @param text The input text containing Slack mentions.
     * @param userInfos Map of userId to UserDTO.
     * @return The text with Slack mentions replaced.
     */
    public static String replaceSlackMentions(String text,  Map<String, UserDTO> userInfos) {
        if (text == null) return null;
        Set<String> keys = userInfos.keySet();
        for (String key : keys) {
            // Replace <@key> with displayName or username
            String mentionPattern = "<@" + Pattern.quote(key) + ">";
            Pattern pattern = Pattern.compile(mentionPattern);
            Matcher matcher = pattern.matcher(text);
            UserDTO user = userInfos.get(key);
            String displayName = user != null && user.displayName() != null ? user.displayName() : (user != null ? user.username() : key);
            text = matcher.replaceAll(displayName);
        }
        return text;
    }
}
