package com.lucid.automation.airouting.util.json;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Utility class for JSON validation operations.
 */
public class JsonValidator {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Validates if a string contains valid JSON structure (parses the JSON).
     *
     * @param jsonString The string to validate
     * @return true if the string contains valid JSON
     */
    public static boolean isValidJsonStructure(String jsonString) {
        if (jsonString == null || jsonString.trim().isEmpty()) {
            return false;
        }
        try {
            objectMapper.readTree(jsonString.trim());
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
