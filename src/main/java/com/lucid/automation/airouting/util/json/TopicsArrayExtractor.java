package com.lucid.automation.airouting.util.json;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for extracting topics arrays from various JSON formats.
 */
public class TopicsArrayExtractor {

    private static final Logger logger = LoggerFactory.getLogger(TopicsArrayExtractor.class);

    /**
     * Attempts to extract topics array using various strategies.
     */
    public static String tryExtractTopicsArray(String trimmed) {
        String jsonTopicsArray = extractTopicsArray(trimmed);
        if (jsonTopicsArray != null && JsonValidator.isValidJsonStructure(jsonTopicsArray)) {
            logger.debug("JSON-CLEAN: Found topics array, returning it directly");
            return jsonTopicsArray;
        }
        
        logger.info("Option2");
        String step2Array = extractArrayAfterStep2(trimmed);
        if (step2Array != null && JsonValidator.isValidJsonStructure(step2Array)) {
            logger.debug("JSON-CLEAN: Found array after 'STEP 2: Topic Details', returning it directly");
            return step2Array;
        }
        
        return null;
    }

    /**
     * Extracts the topics array from a JSON object or multiple JSON objects as a string.
     * Handles both structured JSON with a "topics" field and multiple individual JSON objects.
     */
    private static String extractTopicsArray(String cleaned) {
        if (cleaned == null || cleaned.isEmpty()) return null;

        // Try traditional "topics": [ array structure first
        String traditionalTopics = extractTraditionalTopicsArray(cleaned);
        if (traditionalTopics != null) return traditionalTopics;

        // Try extracting multiple JSON objects after STEP 2
        String step2Objects = extractObjectsAfterStep2(cleaned);
        if (step2Objects != null) return step2Objects;

        // Fallback: try parsing entire input as multiple JSON objects
        return tryParseEntireInputAsMultipleObjects(cleaned);
    }

    /**
     * Extracts traditional "topics": [ array structure.
     */
    private static String extractTraditionalTopicsArray(String cleaned) {
        int topicsIndex = cleaned.indexOf("\"topics\":");
        if (topicsIndex == -1) return null;

        int arrayStart = cleaned.indexOf('[', topicsIndex);
        if (arrayStart == -1) return null;

        int arrayEnd = BracketMatcher.findMatchingBracket(cleaned, arrayStart, '[', ']');
        if (arrayEnd == -1) return null;

        String jsonArray = cleaned.substring(arrayStart, arrayEnd + 1).trim();
        logger.debug("TOPIC-ARRAY-EXTRACT: Extracted topics array of length: {}", jsonArray.length());
        return jsonArray;
    }

    /**
     * Extracts multiple JSON objects after "STEP 2: Topic Details" marker.
     */
    private static String extractObjectsAfterStep2(String cleaned) {
        String step2Marker = "STEP 2: Topic Details";
        int step2Index = cleaned.indexOf(step2Marker);
        if (step2Index == -1) return null;

        String contentAfterStep2 = cleaned.substring(step2Index + step2Marker.length()).trim();
        
        try {
            String multipleObjectsArray = MultipleJsonParser.parseMultipleJsonObjects(contentAfterStep2);
            if (multipleObjectsArray != null && JsonValidator.isValidJsonStructure(multipleObjectsArray)) {
                logger.debug("TOPIC-ARRAY-EXTRACT: Extracted multiple JSON objects as array after STEP 2, length: {}", multipleObjectsArray.length());
                return multipleObjectsArray;
            }
        } catch (Exception e) {
            logger.debug("TOPIC-ARRAY-EXTRACT: Failed to parse multiple JSON objects after STEP 2: {}", e.getMessage());
        }
        
        return null;
    }

    /**
     * Tries to parse the entire input as multiple JSON objects.
     */
    private static String tryParseEntireInputAsMultipleObjects(String cleaned) {
        try {
            String multipleObjectsArray = MultipleJsonParser.parseMultipleJsonObjects(cleaned);
            if (multipleObjectsArray != null && JsonValidator.isValidJsonStructure(multipleObjectsArray)) {
                logger.debug("TOPIC-ARRAY-EXTRACT: Extracted multiple JSON objects as array from entire input, length: {}", multipleObjectsArray.length());
                return multipleObjectsArray;
            }
        } catch (Exception e) {
            logger.debug("TOPIC-ARRAY-EXTRACT: Failed to parse entire input as multiple JSON objects: {}", e.getMessage());
        }

        logger.debug("TOPIC-ARRAY-EXTRACT: No extractable topics array found");
        return null;
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

        int arrayEnd = BracketMatcher.findMatchingBracket(input, arrayStart, '[', ']');
        if (arrayEnd == -1) return null;

        String arrayString = input.substring(arrayStart, arrayEnd + 1).trim();
        logger.debug("STEP2-ARRAY-EXTRACT: Extracted array after 'STEP 2: Topic Details', length={}", arrayString.length());
        return arrayString;
    }
}
