package com.lucid.automation.airouting.util.json;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for extracting JSON content from markdown code blocks.
 */
public class MarkdownExtractor {

    private static final Logger logger = LoggerFactory.getLogger(MarkdownExtractor.class);

    /**
     * Attempts to extract JSON from markdown code blocks.
     */
    public static String tryExtractFromMarkdown(String trimmed) {
        // Check if response contains markdown code blocks with ```json
        if (trimmed.contains("```json")) {
            String extracted = extractFromJsonMarkdown(trimmed);
            if (extracted != null) return extracted;
        }

        // Check if response is wrapped in generic markdown code blocks
        if (trimmed.startsWith("```")) {
            return extractFromGenericMarkdown(trimmed);
        }
        
        return null;
    }

    /**
     * Extracts JSON content from ```json markdown blocks.
     */
    private static String extractFromJsonMarkdown(String trimmed) {
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
        return null;
    }

    /**
     * Extracts JSON content from generic ``` markdown blocks.
     */
    private static String extractFromGenericMarkdown(String trimmed) {
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
        return trimmed;
    }
}
