package com.lucid.automation.airouting.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.ArrayList;

/**
 * Utility class for JSON response processing and cleaning
 * Handles various AI provider response formats including markdown code blocks and prefixed text
 */
public class JsonUtils {

    private static final Logger logger = LoggerFactory.getLogger(JsonUtils.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Cleans JSON response by removing markdown code block formatting if present.
     * Handles responses that start with ```json or ``` and end with ```
     * Also handles responses with prefixed text like "STEP 2: Topic Details"
     *
     * @param rawString The raw response string from AI provider
     * @return Cleaned JSON string ready for parsing
     */
    public static String cleanJsonResponse(String rawString) {
        if (rawString == null || rawString.trim().isEmpty()) {
            logger.warn("JSON-CLEAN: Received null or empty response to clean");
            return rawString;
        }

        String trimmed = rawString.trim();

        String jsonTopicsArray = extractTopicsArray(trimmed);
        if (jsonTopicsArray != null && isValidJsonStructure(jsonTopicsArray)) {
            logger.debug("JSON-CLEAN: Found topics array, returning it directly");
            return jsonTopicsArray;
        } else {
            logger.info("Option2");
            String step2Array = extractArrayAfterStep2(trimmed);
            if (step2Array != null && isValidJsonStructure(step2Array)) {
                logger.debug("JSON-CLEAN: Found array after 'STEP 2: Topic Details', returning it directly");
                return step2Array;
            }
        }

        // Check if response contains markdown code blocks with ```json
        if (trimmed.contains("```json")) {
            logger.debug("JSON-CLEAN: Found ```json markdown block");
            int jsonStart = trimmed.indexOf("```json");
            if (jsonStart >= 0) {
                // Find the first newline after ```json
                int firstNewline = trimmed.indexOf('\n', jsonStart);
                if (firstNewline > 0) {
                    // Extract content after ```json
                    String jsonContent = trimmed.substring(firstNewline + 1);

                    // Find the closing ```
                    int closingIndex = jsonContent.indexOf("```");
                    if (closingIndex > 0) {
                        jsonContent = jsonContent.substring(0, closingIndex);
                    }

                    logger.debug("JSON-CLEAN: Extracted JSON from markdown code block, length: {}", jsonContent.length());
                    return jsonContent.trim();
                }
            }
        }

        // Check if response is wrapped in markdown code blocks
        if (trimmed.startsWith("```")) {
            logger.debug("JSON-CLEAN: Found generic markdown code block");
            // Find the first newline after the opening ```
            int firstNewline = trimmed.indexOf('\n');
            if (firstNewline > 0) {
                // Remove the opening ``` line
                trimmed = trimmed.substring(firstNewline + 1);
            }

            // Remove closing ``` if present
            if (trimmed.endsWith("```")) {
                trimmed = trimmed.substring(0, trimmed.length() - 3).trim();
            }
            logger.debug("JSON-CLEAN: Removed markdown formatting, new length: {}", trimmed.length());
        }

        // Look for JSON content by finding the first { or [
        int jsonStart = -1;
        for (int i = 0; i < trimmed.length(); i++) {
            char c = trimmed.charAt(i);
            if (c == '{' || c == '[') {
                jsonStart = i;
                break;
            }
        }

        if (jsonStart > 0) {
            // Found JSON content after some text (like "STEP 2: Topic Details"), extract from that point
            String prefixText = trimmed.substring(0, jsonStart).trim();
            trimmed = trimmed.substring(jsonStart);

            // Check for specific known patterns
            if (prefixText.contains("STEP") && prefixText.contains("Topic Details")) {
                logger.debug("JSON-CLEAN: Found 'STEP X: Topic Details' pattern, extracting JSON content");
            } else if (prefixText.contains("STEP")) {
                logger.debug("JSON-CLEAN: Found 'STEP' pattern: '{}'", prefixText);
            }

            logger.debug("JSON-CLEAN: Found JSON after prefix text '{}', extracted JSON content of length: {}",
                    prefixText.length() > 50 ? prefixText.substring(0, 50) + "..." : prefixText,
                    trimmed.length());
        } else if (jsonStart == -1) {
            // No JSON structure found, log the response for debugging
            logger.warn("JSON-CLEAN: No JSON structure found in response (length: {}): {}",
                    trimmed.length(),
                    trimmed.length() > 200 ? trimmed.substring(0, 200) + "..." : trimmed);
            return trimmed; // Return as-is and let the parsing fail gracefully
        }

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
            trimmed = trimmed.substring(0, jsonEnd + 1);
            logger.debug("JSON-CLEAN: Removed trailing text '{}' after JSON, final length: {}",
                    trailingText.length() > 50 ? trailingText.substring(0, 50) + "..." : trailingText,
                    trimmed.length());
        }

        // try to parse the cleaned response to ensure it's valid JSON
        String jsonText = parseMultipleJsonObjects(trimmed);

        logger.debug("JSON-CLEAN: Cleaning completed, final response length: {}", trimmed.length());
        return jsonText.trim();
    }

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

    /**
     * Extracts the topics array from a JSON object as a string.
     */
    private static String extractTopicsArray(String cleaned) {
        if (cleaned == null || cleaned.isEmpty()) return null;

        // Look for the "topics": [ and extract the JSON array that follows
        int startIndex = cleaned.indexOf("\"topics\":");
        if (startIndex == -1) return null;

        int arrayStart = cleaned.indexOf('[', startIndex);
        if (arrayStart == -1) return null;

        int depth = 0;
        int arrayEnd = -1;
        for (int i = arrayStart; i < cleaned.length(); i++) {
            char c = cleaned.charAt(i);
            if (c == '[') depth++;
            else if (c == ']') {
                depth--;
                if (depth == 0) {
                    arrayEnd = i;
                    break;
                }
            }
        }

        if (arrayEnd == -1) return null;

        String jsonArray = cleaned.substring(arrayStart, arrayEnd + 1).trim();
        logger.debug("TOPIC-ARRAY-EXTRACT: Extracted topics array of length: {}", jsonArray.length());
        return jsonArray;
    }

    /**
     * Extracts the first JSON array found after the 'STEP 2: Topic Details' marker.
     */
    public static String extractArrayAfterStep2(String input) {
        if (input == null || input.isEmpty()) return null;

        String marker = "STEP 2: Topic Details";
        int markerIndex = input.indexOf(marker);
        if (markerIndex == -1) return null;

        // Start searching for '[' after the marker
        int arrayStart = input.indexOf('[', markerIndex);
        if (arrayStart == -1) return null;

        int depth = 0;
        int arrayEnd = -1;
        for (int i = arrayStart; i < input.length(); i++) {
            char c = input.charAt(i);
            if (c == '[') depth++;
            else if (c == ']') {
                depth--;
                if (depth == 0) {
                    arrayEnd = i;
                    break;
                }
            }
        }

        if (arrayEnd == -1) return null;

        String arrayString = input.substring(arrayStart, arrayEnd + 1).trim();
        logger.debug("STEP2-ARRAY-EXTRACT: Extracted array after 'STEP 2: Topic Details', length={}", arrayString.length());
        return arrayString;
    }

    /**
     * Parses a string containing one or more JSON objects (with or without commas between them)
     * into an array of JSON objects.
     * 
     * Examples of supported formats:
     * - Single object: {"key": "value"}
     * - Multiple objects with commas: {"key1": "value1"}, {"key2": "value2"}
     * - Multiple objects without commas: {"key1": "value1"} {"key2": "value2"}
     * - Mixed whitespace and newlines between objects
     * 
     * @param input The input string containing JSON objects
     * @return String representation of a JSON array containing all parsed objects
     * @throws IllegalArgumentException if no valid JSON objects are found
     */
    public static String parseMultipleJsonObjects(String input) {
        if (input == null || input.trim().isEmpty()) {
            logger.warn("MULTI-JSON-PARSE: Received null or empty input");
            throw new IllegalArgumentException("Input string cannot be null or empty");
        }

        String cleaned = input.trim();
        logger.debug("MULTI-JSON-PARSE: Processing input of length: {}", cleaned.length());

        // If input is already a valid JSON array, return it as-is
        if (cleaned.startsWith("[") && cleaned.endsWith("]")) {
            if (isValidJsonStructure(cleaned)) {
                logger.debug("MULTI-JSON-PARSE: Input is already a valid JSON array");
                return cleaned;
            }
        }

        // Extract all JSON objects from the string
        List<String> jsonObjects = extractJsonObjects(cleaned);
        
        if (jsonObjects.isEmpty()) {
            logger.warn("MULTI-JSON-PARSE: No valid JSON objects found in input");
            throw new IllegalArgumentException("No valid JSON objects found in input string");
        }

        // Build the JSON array string
        StringBuilder arrayBuilder = new StringBuilder("[");
        for (int i = 0; i < jsonObjects.size(); i++) {
            if (i > 0) {
                arrayBuilder.append(",");
            }
            arrayBuilder.append(jsonObjects.get(i));
        }
        arrayBuilder.append("]");

        String result = arrayBuilder.toString();
        logger.debug("MULTI-JSON-PARSE: Successfully parsed {} JSON objects into array", jsonObjects.size());
        
        // Validate the final result
        if (!isValidJsonStructure(result)) {
            logger.error("MULTI-JSON-PARSE: Generated array is not valid JSON");
            throw new IllegalArgumentException("Failed to create valid JSON array from input objects");
        }

        return result;
    }

    /**
     * Extracts individual JSON objects from a string that may contain multiple objects
     * with or without commas between them.
     * 
     * @param input The input string containing JSON objects
     * @return List of individual JSON object strings
     */
    private static List<String> extractJsonObjects(String input) {
        List<String> objects = new ArrayList<>();
        int i = 0;
        
        while (i < input.length()) {
            // Skip whitespace and commas
            while (i < input.length() && (Character.isWhitespace(input.charAt(i)) || input.charAt(i) == ',')) {
                i++;
            }
            
            if (i >= input.length()) break;
            
            // Look for the start of a JSON object
            if (input.charAt(i) == '{') {
                int objectStart = i;
                int depth = 0;
                int objectEnd = -1;
                
                // Find the matching closing brace
                while (i < input.length()) {
                    char c = input.charAt(i);
                    if (c == '{') {
                        depth++;
                    } else if (c == '}') {
                        depth--;
                        if (depth == 0) {
                            objectEnd = i;
                            break;
                        }
                    } else if (c == '"') {
                        // Skip string content to avoid counting braces inside strings
                        i++;
                        while (i < input.length() && input.charAt(i) != '"') {
                            if (input.charAt(i) == '\\') {
                                i++; // Skip escaped character
                            }
                            i++;
                        }
                    }
                    i++;
                }
                
                if (objectEnd != -1) {
                    String jsonObject = input.substring(objectStart, objectEnd + 1).trim();
                    
                    // Validate the extracted object
                    if (isValidJsonStructure(jsonObject)) {
                        objects.add(jsonObject);
                        logger.debug("MULTI-JSON-EXTRACT: Found valid JSON object at position {}-{}", objectStart, objectEnd);
                    } else {
                        logger.warn("MULTI-JSON-EXTRACT: Invalid JSON object found at position {}-{}: {}", 
                                   objectStart, objectEnd, 
                                   jsonObject.length() > 100 ? jsonObject.substring(0, 100) + "..." : jsonObject);
                    }
                    i = objectEnd + 1;
                } else {
                    logger.warn("MULTI-JSON-EXTRACT: Unclosed JSON object found starting at position {}", objectStart);
                    break;
                }
            } else {
                // Skip non-JSON content
                i++;
            }
        }
        
        logger.debug("MULTI-JSON-EXTRACT: Extracted {} valid JSON objects", objects.size());
        return objects;
    }

    
}
