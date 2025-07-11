package com.lucid.automation.airouting.util.json;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main utility class for JSON response processing and cleaning.
 * Handles various AI provider response formats including markdown code blocks and prefixed text.
 */
public class JsonCleaner {

    private static final Logger logger = LoggerFactory.getLogger(JsonCleaner.class);

    /**
     * Cleans JSON response by removing markdown code block formatting if present.
     * Handles responses that start with ```json or ``` and end with ```
     * Also handles responses with prefixed text like "STEP 2: Topic Details"
     * First attempts to decode JSON-encoded strings (strings wrapped in quotes with escaped characters).
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
        
        // First, try to decode if it's a JSON-encoded string
        String decoded = JsonStringDecoder.decodeJsonString(trimmed);
        if (!decoded.equals(trimmed)) {
            logger.debug("JSON-CLEAN: Successfully decoded JSON-encoded string");
            trimmed = decoded;
        }

        // Try to extract topics array first
        String topicsArray = TopicsArrayExtractor.tryExtractTopicsArray(trimmed);
        if (topicsArray != null) {
            return topicsArray;
        }

        // Try markdown code block extraction
        String markdownExtracted = MarkdownExtractor.tryExtractFromMarkdown(trimmed);
        if (markdownExtracted != null) {
            return markdownExtracted;
        }

        // Handle prefix text and trailing text
        String cleanedJson = TextAroundJsonRemover.removeTextAroundJson(trimmed);
        
        // Final parsing attempt
        return MultipleJsonParser.parseMultipleJsonObjects(cleanedJson).trim();
    }
}
