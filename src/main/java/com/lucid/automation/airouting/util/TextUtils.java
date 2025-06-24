package com.lucid.automation.airouting.util;

import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lucid.automation.airouting.dto.UserDTO;

public class TextUtils {
    
    // Private constructor to prevent instantiation
    private TextUtils() {
        throw new AssertionError("Utility class should not be instantiated");
    }

    private static final Logger logger = LoggerFactory.getLogger(TextUtils.class);
    
    /**
     * Replaces Slack mentions (e.g., <@U12345>) in the input text with the display name or username from userInfos.
     * @param text The input text containing Slack mentions.
     * @param userInfos Map of userId to UserDTO.
     * @return The text with Slack mentions replaced.
     */
    public static String replaceSlackMentions(String text,  Map<String, UserDTO> userInfos) {
        if (text == null) return null;
        if (userInfos == null || userInfos.isEmpty()) return text;
        
        Set<String> keys = userInfos.keySet();
        for (String key : keys) {
            // Skip null or empty keys
            if (key == null || key.trim().isEmpty()) {
                logger.warn("Found null or empty key in userInfos map, skipping");
                continue;
            }
            
            try {
                // Replace <@key> with displayName or username
                String mentionPattern = "\\s*(<@" + Pattern.quote(key) + ">|" + Pattern.quote(key) + ")\\s*";
                Pattern pattern = Pattern.compile(mentionPattern);
                Matcher matcher = pattern.matcher(text);
                UserDTO user = userInfos.get(key);
                String displayName = user != null && user.displayName() != null ? user.displayName() : (user != null ? user.username() : key);
                text = matcher.replaceAll(displayName);
            } catch (Exception e) {
                // Log and continue with next key if pattern compilation fails
                logger.warn("Failed to process mention for key: {}, error: {}", key, e.getMessage());
            }
        }
        return text;
    }
}
