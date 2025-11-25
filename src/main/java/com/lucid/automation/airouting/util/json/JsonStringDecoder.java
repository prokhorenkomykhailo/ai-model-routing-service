package com.lucid.automation.airouting.util.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for decoding JSON-encoded strings.
 * Handles cases where the content is wrapped in quotes with escaped characters.
 */
public class JsonStringDecoder {

    private static final Logger logger = LoggerFactory.getLogger(JsonStringDecoder.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Decodes a JSON-encoded string by removing outer quotes and unescaping characters.
     *
     * @param jsonEncodedString The JSON-encoded string to decode
     * @return The decoded string, or the original string if decoding fails
     */
    public static String decodeJsonString(String jsonEncodedString) {
        if (jsonEncodedString == null || jsonEncodedString.trim().isEmpty()) {
            return jsonEncodedString;
        }

        String trimmed = jsonEncodedString.trim();

        // Check if the string is wrapped in quotes (JSON-encoded)
        if (isJsonEncodedString(trimmed)) {
            try {
                // Use Jackson's ObjectMapper to properly decode the JSON string
                return objectMapper.readValue(trimmed, String.class);
            } catch (JsonProcessingException e) {
                logger.warn("🔧 Failed to decode JSON string using Jackson, falling back to manual decoding: {}", e.getMessage());
                // Fallback to manual decoding
                return manualDecodeJsonString(trimmed);
            }
        }

        return jsonEncodedString;
    }

    /**
     * Checks if the given string appears to be a JSON-encoded string.
     *
     * @param str The string to check
     * @return true if the string appears to be JSON-encoded, false otherwise
     */
    private static boolean isJsonEncodedString(String str) {
        if (str.length() < 2) {
            return false;
        }

        // Check if string starts and ends with quotes
        if (str.startsWith("\"") && str.endsWith("\"")) {
            // Check if it contains escaped characters typical of JSON encoding
            return str.contains("\\n") || str.contains("\\\"") || str.contains("\\\\") || str.contains("\\t");
        }

        return false;
    }

    /**
     * Manual decoding of JSON string as a fallback method.
     *
     * @param jsonString The JSON string to decode
     * @return The decoded string
     */
    private static String manualDecodeJsonString(String jsonString) {
        if (jsonString.length() < 2) {
            return jsonString;
        }

        // Remove outer quotes
        String content = jsonString.substring(1, jsonString.length() - 1);

        // Unescape common JSON escape sequences
        content = content
                .replace("\\\"", "\"")
                .replace("\\\\", "\\")
                .replace("\\n", "\n")
                .replace("\\r", "\r")
                .replace("\\t", "\t")
                .replace("\\b", "\b")
                .replace("\\f", "\f")
                .replace("\\/", "/");

        return content;
    }

    /**
     * Attempts to decode a JSON string and returns information about the result.
     *
     * @param jsonEncodedString The JSON-encoded string to decode
     * @return DecodingResult containing the decoded string and metadata
     */
    public static DecodingResult decodeWithMetadata(String jsonEncodedString) {
        if (jsonEncodedString == null || jsonEncodedString.trim().isEmpty()) {
            return new DecodingResult(jsonEncodedString, false, "Input is null or empty");
        }

        String trimmed = jsonEncodedString.trim();
        boolean wasEncoded = isJsonEncodedString(trimmed);

        if (wasEncoded) {
            try {
                String decoded = objectMapper.readValue(trimmed, String.class);
                return new DecodingResult(decoded, true, "Successfully decoded using Jackson");
            } catch (JsonProcessingException e) {
                logger.warn("Jackson decoding failed, using manual decoding: {}", e.getMessage());
                String decoded = manualDecodeJsonString(trimmed);
                return new DecodingResult(decoded, true, "Successfully decoded using manual method");
            }
        } else {
            return new DecodingResult(jsonEncodedString, false, "String was not JSON-encoded");
        }
    }

    /**
     * Result class for decoding operations.
     */
    public static class DecodingResult {
        private final String decodedString;
        private final boolean wasDecoded;
        private final String message;

        public DecodingResult(String decodedString, boolean wasDecoded, String message) {
            this.decodedString = decodedString;
            this.wasDecoded = wasDecoded;
            this.message = message;
        }

        public String getDecodedString() {
            return decodedString;
        }

        public boolean wasDecoded() {
            return wasDecoded;
        }

        public String getMessage() {
            return message;
        }

        @Override
        public String toString() {
            return String.format("DecodingResult{wasDecoded=%s, message='%s'}", wasDecoded, message);
        }
    }
}
