package com.lucid.automation.airouting.util;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lucid.automation.common.dto.enrichment.EnrichmentUserDTO;

public class TextUtils {

    // Private constructor to prevent instantiation
    private TextUtils() {
        throw new AssertionError("Utility class should not be instantiated");
    }

    private static final Logger logger = LoggerFactory.getLogger(TextUtils.class);

    /**
     * Replaces Slack mentions (e.g., <@U12345>) in the input text with the display name or username from userInfos.
     * @param text The input text containing Slack mentions.
     * @param userInfos Map of userId to EnrichmentUserDTO.
     * @return The text with Slack mentions replaced.
     */
    public static String replaceSlackMentions(String text, Map<String, EnrichmentUserDTO> userInfos) {
        if (text == null) return null;
        if (userInfos == null || userInfos.isEmpty()) return text;

        // Pattern to match Slack mentions like <@U02FB4HRF>
        Pattern pattern = Pattern.compile("<@([A-Z0-9]+)>");
        Matcher matcher = pattern.matcher(text);

        while (matcher.find()) {
            String key = matcher.group(1);
            try {
                EnrichmentUserDTO user = userInfos.get(key);
                String name;
                if (user != null) {
                    if (user.displayName() != null) {
                        name = user.displayName();
                    } else if (user.username() != null) {
                        name = user.username();
                    } else {
                        name = key;
                    }
                } else {
                    name = key;
                }

                // Thay thế bằng " name " (một dấu cách hai bên)
                text = matcher.replaceAll(" " + name + " ");

            } catch (Exception e) {
                logger.warn("👤 Failed to process mention for key: {}, error: {}", key, e.getMessage());
            }
        }

        // Gộp các cụm >=2 dấu cách thành 1, rồi trim hai đầu
        text = text.replaceAll("\\s{2,}", " ").trim();
        return text;
    }
}
