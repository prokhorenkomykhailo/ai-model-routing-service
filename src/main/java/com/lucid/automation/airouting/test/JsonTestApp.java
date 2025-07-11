package com.lucid.automation.airouting.test;

import com.lucid.automation.airouting.util.json.JsonCleaner;
import com.lucid.automation.airouting.util.json.JsonStringDecoder;
import com.lucid.automation.airouting.util.json.JsonValidator;
import com.lucid.automation.airouting.util.json.MultipleJsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Test application for JSON parsing utilities.
 * Reads a file from resources and processes it using the JSON utilities.
 */
public class JsonTestApp {

    private static final Logger logger = LoggerFactory.getLogger(JsonTestApp.class);

    public static void main(String[] args) {
        JsonTestApp app = new JsonTestApp();
        
        if (args.length > 0) {
            // Use file path from command line argument
            app.processFile(args[0]);
        } else {
            // Use default file in project directory
            app.processFile("test-data.txt");
        }
    }

    /**
     * Processes a file by reading its content and parsing it as JSON.
     *
     * @param filePath Path to the file to process
     */
    public void processFile(String filePath) {
        try {
            // Read file content
            String content = readFileContent(filePath);
            if (content == null) {
                return;
            }

            System.out.println("=== JSON Test Application ===");
            System.out.println("File: " + filePath);
            System.out.println("Content length: " + content.length() + " characters");
            System.out.println();

            // // Show original content (truncated if too long)
            // displayOriginalContent(content);

            // // Test JSON string decoding
            // testJsonStringDecoding(content);

            // Test JSON cleaning
            testJsonCleaning(content);

            // Test JSON validation
            // testJsonValidation(content);

            // // Test multiple JSON parsing
            // testMultipleJsonParsing(content);

            // // Test JSON string decoding
            // testJsonStringDecoding(content);

        } catch (Exception e) {
            logger.error("Error processing file: {}", filePath, e);
            System.err.println("Error processing file: " + e.getMessage());
        }
    }

    /**
     * Reads the content of a file.
     *
     * @param filePath Path to the file to read
     * @return File content as string, or null if failed
     */
    private String readFileContent(String filePath) {
        try {
            Path path = Paths.get(filePath);
            if (!Files.exists(path)) {
                System.err.println("File not found: " + filePath);
                System.err.println("Current working directory: " + System.getProperty("user.dir"));
                return null;
            }

            return Files.readString(path);
        } catch (IOException e) {
            logger.error("Failed to read file: {}", filePath, e);
            System.err.println("Failed to read file: " + e.getMessage());
            return null;
        }
    }

    /**
     * Displays the original content (truncated if necessary).
     */
    private void displayOriginalContent(String content) {
        System.out.println("=== Original Content ===");
        if (content.length() > 500) {
            System.out.println(content.substring(0, 500) + "...");
            System.out.println("(Content truncated for display)");
        } else {
            System.out.println(content);
        }
        System.out.println();
    }

    /**
     * Tests JSON cleaning functionality.
     */
    private void testJsonCleaning(String content) {
        System.out.println("=== JSON Cleaning Test ===");
        try {
            String cleanedJson = JsonCleaner.cleanJsonResponse(content);
            System.out.println("✓ JSON cleaning successful");
            System.out.println("Cleaned JSON length: " + cleanedJson.length() + " characters");
            
            // Show cleaned JSON (full output)
            System.out.println("Cleaned JSON (full output):");
            System.out.println(cleanedJson);
            
        } catch (Exception e) {
            System.out.println("✗ JSON cleaning failed: " + e.getMessage());
            logger.error("JSON cleaning failed", e);
        }
        System.out.println();
    }

    /**
     * Tests JSON validation functionality.
     */
    private void testJsonValidation(String content) {
        System.out.println("=== JSON Validation Test ===");
        
        // Test original content
        boolean isOriginalValid = JsonValidator.isValidJsonStructure(content);
        System.out.println("Original content valid JSON: " + (isOriginalValid ? "✓ Yes" : "✗ No"));
        
        // Test cleaned content
        try {
            String cleanedJson = JsonCleaner.cleanJsonResponse(content);
            boolean isCleanedValid = JsonValidator.isValidJsonStructure(cleanedJson);
            System.out.println("Cleaned content valid JSON: " + (isCleanedValid ? "✓ Yes" : "✗ No"));
            
            if (isCleanedValid) {
                System.out.println("✓ JSON validation successful - content is valid JSON after cleaning");
            } else {
                System.out.println("✗ JSON validation failed - content is not valid JSON even after cleaning");
            }
            
        } catch (Exception e) {
            System.out.println("✗ JSON validation test failed: " + e.getMessage());
            logger.error("JSON validation test failed", e);
        }
        System.out.println();
    }

    /**
     * Tests multiple JSON parsing functionality.
     */
    private void testMultipleJsonParsing(String content) {
        System.out.println("=== Multiple JSON Parsing Test ===");
        try {
            // First clean the content
            String cleanedJson = JsonCleaner.cleanJsonResponse(content);
            
            // Try to parse as multiple JSON objects
            String jsonArray = MultipleJsonParser.parseMultipleJsonObjects(cleanedJson);
            System.out.println("✓ Multiple JSON parsing successful");
            System.out.println("Result JSON array length: " + jsonArray.length() + " characters");
            
            // Show result (truncated if too long)
            if (jsonArray.length() > 300) {
                System.out.println("Result JSON array (first 300 chars):");
                System.out.println(jsonArray.substring(0, 300) + "...");
            } else {
                System.out.println("Result JSON array:");
                System.out.println(jsonArray);
            }
            
            // Validate the result
            boolean isResultValid = JsonValidator.isValidJsonStructure(jsonArray);
            System.out.println("Result is valid JSON: " + (isResultValid ? "✓ Yes" : "✗ No"));
            
        } catch (Exception e) {
            System.out.println("✗ Multiple JSON parsing failed: " + e.getMessage());
            logger.error("Multiple JSON parsing failed", e);
        }
        System.out.println();
    }

    /**
     * Tests JSON string decoding functionality.
     */
    private void testJsonStringDecoding(String content) {
        System.out.println("=== JSON String Decoding Test ===");
        try {
            JsonStringDecoder.DecodingResult result = JsonStringDecoder.decodeWithMetadata(content);
            System.out.println("✓ JSON string decoding completed");
            System.out.println("Was content JSON-encoded: " + (result.wasDecoded() ? "✓ Yes" : "✗ No"));
            System.out.println("Decoding message: " + result.getMessage());
            
            if (result.wasDecoded()) {
                System.out.println("Decoded content length: " + result.getDecodedString().length() + " characters");
                System.out.println("Original vs Decoded length: " + content.length() + " -> " + result.getDecodedString().length());
            }
            
        } catch (Exception e) {
            System.out.println("✗ JSON string decoding failed: " + e.getMessage());
            logger.error("JSON string decoding failed", e);
        }
        System.out.println();
    }
}
