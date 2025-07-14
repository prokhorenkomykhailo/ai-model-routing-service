package com.lucid.automation.airouting.provider;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import com.lucid.automation.common.dto.enrichment.SuggestedReply;

public class ProviderUtils {

    // Constants for suggested replies fields
    private static final String SUGGESTED_REPLIES_KEY = "suggestedReplies";
    private static final String TONE_KEY = "tone";
    private static final String REPLY_METHOD_KEY = "replyMethod";
    private static final String RECIPIENT_HANDLE_KEY = "recipientHandle";
    private static final String CHANNEL_NAME_KEY = "channelName";
    private static final String CHANNEL_ID_KEY = "channelId";
    private static final String THREAD_ID_KEY = "threadId";
    private static final String TO_KEY = "to";
    private static final String CC_KEY = "cc";
    private static final String SUBJECT_KEY = "subject";
    private static final String MESSAGE_BODY_KEY = "messageBody";
    private static final String DEFAULT_MESSAGE_BODY = "";

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
        Object suggestedRepliesObj = topicMap.get(SUGGESTED_REPLIES_KEY);
        if (suggestedRepliesObj instanceof List<?> repliesList) {
            List<SuggestedReply> replies = new ArrayList<>();

            for (Object replyObj : repliesList) {
                if (replyObj instanceof Map<?, ?> replyMap) {
                    String tone = extractStringValue(replyMap, TONE_KEY, null);
                    String replyMethod = extractStringValue(replyMap, REPLY_METHOD_KEY, null);
                    String recipientHandle = extractStringValue(replyMap, RECIPIENT_HANDLE_KEY, null);
                    String channelName = extractStringValue(replyMap, CHANNEL_NAME_KEY, null);
                    String channelId = extractStringValue(replyMap, CHANNEL_ID_KEY, null);
                    String threadId = extractStringValue(replyMap, THREAD_ID_KEY, null);
                    String to = extractStringValue(replyMap, TO_KEY, null);
                    List<String> cc = extractStringList(replyMap, CC_KEY);
                    String subject = extractStringValue(replyMap, SUBJECT_KEY, null);
                    String messageBody = extractStringValue(replyMap, MESSAGE_BODY_KEY, DEFAULT_MESSAGE_BODY);

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
