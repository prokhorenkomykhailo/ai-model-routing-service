package com.lucid.automation.airouting.provider;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import com.lucid.automation.airouting.dto.SuggestedReply;

public class ProviderUtils {

    public static String extractStringValue(Map<?, ?> map, String key, String defaultValue) {
        Object value = map.get(key);
        return value instanceof String str ? str : defaultValue;
    }

    public static List<String> extractStringList(Map<?, ?> map, String key) {
        Object value = map.get(key);
        if (value instanceof List<?> list) {
            return list.stream()
                .filter(String.class::isInstance)
                .map(String.class::cast)
                .toList();
        }
        return List.of();
    }

    /**
     * Extracts numeric value from a string that may contain descriptive text.
     * Handles formats like:
     * - "0.8"
     * - "0.8 (High confidence)"
     * - "0.2 - Low value with description"
     */
    public static double parseNumericValue(String valueStr, double defaultValue) {
        try {
            // First, try to parse directly in case it's just a number
            return Double.parseDouble(valueStr);
        } catch (NumberFormatException e) {
            // If that fails, extract the first numeric value from the string
            try {
                // Use regex to find first decimal number (including integers)
                java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("([0-9]*\\.?[0-9]+)");
                java.util.regex.Matcher matcher = pattern.matcher(valueStr);
                
                if (matcher.find()) {
                    double value = Double.parseDouble(matcher.group(1));
                    return value;
                } else {
                    return defaultValue; // Default fallback
                }
            } catch (Exception parseException) {
                return defaultValue; // Default fallback
            }
        }
    }

    public static List<SuggestedReply> extractSuggestedReplies(Map<?, ?> topicMap) {
        Object suggestedRepliesObj = topicMap.get("suggestedReplies");
        if (suggestedRepliesObj instanceof List<?> repliesList) {
            List<SuggestedReply> replies = new ArrayList<>();

            for (Object replyObj : repliesList) {
                if (replyObj instanceof Map<?, ?> replyMap) {
                    String tone = extractStringValue(replyMap, "tone", null);
                    String replyMethod = extractStringValue(replyMap, "replyMethod", null);
                    String recipientHandle = extractStringValue(replyMap, "recipientHandle", null);
                    String channelName = extractStringValue(replyMap, "channelName", null);
                    String channelId = extractStringValue(replyMap, "channelId", null);
                    String threadId = extractStringValue(replyMap, "threadId", null);
                    String to = extractStringValue(replyMap, "to", null);
                    List<String> cc = extractStringList(replyMap, "cc");
                    String subject = extractStringValue(replyMap, "subject", null);
                    String messageBody = extractStringValue(replyMap, "messageBody", "");

                    SuggestedReply suggestedReply = new SuggestedReply(
                        tone, replyMethod, recipientHandle, channelName, channelId,
                        threadId, to, cc, subject, messageBody
                    );
                    replies.add(suggestedReply);
                }
            }
            return replies;
        }
        return List.of();
    }
}
