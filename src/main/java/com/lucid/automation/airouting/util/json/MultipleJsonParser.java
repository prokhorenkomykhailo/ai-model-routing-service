package com.lucid.automation.airouting.util.json;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.ArrayList;

/**
 * Utility class for parsing multiple JSON objects into a single JSON array.
 */
public class MultipleJsonParser {

    private static final Logger logger = LoggerFactory.getLogger(MultipleJsonParser.class);

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
            if (JsonValidator.isValidJsonStructure(cleaned)) {
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
        if (!JsonValidator.isValidJsonStructure(result)) {
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
            i = skipWhitespaceAndCommas(input, i);
            if (i >= input.length()) break;
            
            if (input.charAt(i) == '{') {
                JsonObjectExtraction extraction = extractSingleJsonObject(input, i);
                if (extraction.isValid()) {
                    objects.add(extraction.getJsonObject());
                    logger.debug("MULTI-JSON-EXTRACT: Found valid JSON object at position {}-{}", 
                               extraction.getStart(), extraction.getEnd());
                } else {
                    logger.warn("MULTI-JSON-EXTRACT: Invalid JSON object found at position {}-{}: {}", 
                               extraction.getStart(), extraction.getEnd(), 
                               extraction.getTruncatedObject());
                }
                i = extraction.getNextPosition();
            } else {
                i++; // Skip non-JSON content
            }
        }
        
        logger.debug("MULTI-JSON-EXTRACT: Extracted {} valid JSON objects", objects.size());
        return objects;
    }

    /**
     * Skips whitespace and comma characters.
     */
    private static int skipWhitespaceAndCommas(String input, int startIndex) {
        while (startIndex < input.length() && 
               (Character.isWhitespace(input.charAt(startIndex)) || input.charAt(startIndex) == ',')) {
            startIndex++;
        }
        return startIndex;
    }

    /**
     * Extracts a single JSON object starting from the given position.
     */
    private static JsonObjectExtraction extractSingleJsonObject(String input, int objectStart) {
        int i = objectStart;
        int depth = 0;
        int objectEnd = -1;
        
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
                i = skipStringContent(input, i);
                if (i >= input.length()) break;
            }
            i++;
        }
        
        return new JsonObjectExtraction(input, objectStart, objectEnd, i + 1);
    }

    /**
     * Skips string content to avoid counting braces inside strings.
     */
    private static int skipStringContent(String input, int stringStart) {
        int i = stringStart + 1; // Skip opening quote
        while (i < input.length() && input.charAt(i) != '"') {
            if (input.charAt(i) == '\\') {
                i++; // Skip escaped character
            }
            i++;
        }
        return i; // Position of closing quote
    }
}
