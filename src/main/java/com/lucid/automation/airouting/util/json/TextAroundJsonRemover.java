package com.lucid.automation.airouting.util.json;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for removing text around JSON content.
 */
public class TextAroundJsonRemover {

    private static final Logger logger = LoggerFactory.getLogger(TextAroundJsonRemover.class);

    /**
     * Removes prefix and trailing text around JSON content.
     */
    public static String removeTextAroundJson(String trimmed) {
        // Look for JSON content by finding the first { or [
        int jsonStart = findJsonStart(trimmed);
        
        if (jsonStart > 0) {
            trimmed = handlePrefixText(trimmed, jsonStart);
        } else if (jsonStart == -1) {
            // No JSON structure found, log the response for debugging
            logger.warn("JSON-CLEAN: No JSON structure found in response (length: {}): {}",
                    trimmed.length(),
                    trimmed.length() > 200 ? trimmed.substring(0, 200) + "..." : trimmed);
            return trimmed; // Return as-is and let the parsing fail gracefully
        }

        // Remove trailing text
        return removeTrailingText(trimmed);
    }

    /**
     * Finds the start position of JSON content in the string.
     */
    private static int findJsonStart(String trimmed) {
        for (int i = 0; i < trimmed.length(); i++) {
            char c = trimmed.charAt(i);
            if (c == '{' || c == '[') {
                return i;
            }
        }
        return -1;
    }

    /**
     * Handles prefix text before JSON content.
     */
    private static String handlePrefixText(String trimmed, int jsonStart) {
        String prefixText = trimmed.substring(0, jsonStart).trim();
        String extractedJson = trimmed.substring(jsonStart);

        // Check for specific known patterns
        if (prefixText.contains("STEP") && prefixText.contains("Topic Details")) {
            logger.debug("JSON-CLEAN: Found 'STEP X: Topic Details' pattern, extracting JSON content");
        } else if (prefixText.contains("STEP")) {
            logger.debug("JSON-CLEAN: Found 'STEP' pattern: '{}'", prefixText);
        }

        logger.debug("JSON-CLEAN: Found JSON after prefix text '{}', extracted JSON content of length: {}",
                prefixText.length() > 50 ? prefixText.substring(0, 50) + "..." : prefixText,
                extractedJson.length());
        
        return extractedJson;
    }

    /**
     * Removes trailing text after JSON content.
     */
    private static String removeTrailingText(String trimmed) {
        // Find the last } or ] to handle any trailing text
        int jsonEnd = -1;
        for (int i = trimmed.length() - 1; i >= 0; i--) {
            char c = trimmed.charAt(i);
            if (c == '}' || c == ']') {
                jsonEnd = i;
                break;
            }
        }

        if (jsonEnd > 0 && jsonEnd < trimmed.length() - 1) {
            // Found trailing text after JSON, remove it
            String trailingText = trimmed.substring(jsonEnd + 1).trim();
            String cleanedJson = trimmed.substring(0, jsonEnd + 1);
            logger.debug("JSON-CLEAN: Removed trailing text '{}' after JSON, final length: {}",
                    trailingText.length() > 50 ? trailingText.substring(0, 50) + "..." : trailingText,
                    cleanedJson.length());
            return cleanedJson;
        }
        
        return trimmed;
    }
}
