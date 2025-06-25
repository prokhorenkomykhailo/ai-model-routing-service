package com.lucid.automation.airouting.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class PromptLoader {
    
    private static final Logger logger = LoggerFactory.getLogger(PromptLoader.class);
    
    // Cache for loaded prompts
    private final Map<String, String> promptCache = new ConcurrentHashMap<>();
    
    /**
     * Load a prompt template from a markdown file in the resources/prompts directory.
     * The prompt is cached after first load for performance.
     */
    public String loadPromptTemplate(String promptName) {
        return promptCache.computeIfAbsent(promptName, name -> {
            try {
                String fileName = "prompts/" + name + ".md";
                ClassPathResource resource = new ClassPathResource(fileName);
                
                if (!resource.exists()) {
                    logger.warn("Prompt file not found: {}", fileName);
                    return getDefaultPrompt(name);
                }
                
                String content = resource.getContentAsString(StandardCharsets.UTF_8);
                String[] lines = content.split("\n");
                StringBuilder promptContent = new StringBuilder();
                
                for (String line : lines) {
                    if (line.trim().isEmpty()) {
                        continue;
                    }
                    promptContent.append(line).append("\n");
                }
                return promptContent.toString().trim();
            } catch (IOException e) {
                logger.error("Failed to load prompt template '{}': {}", name, e.getMessage(), e);
                return getDefaultPrompt(name);
            }
        });
    }
    
    /**
     * Get a default prompt if loading from file fails
     */
    private String getDefaultPrompt(String promptName) {
        return switch (promptName) {
            case "categorization" -> "Categorize this message: \"%s\"";
            case "summarization" -> "Summarize this content: \"%s\"";  
            case "conversation-enrichment" -> "Analyze this conversation: %s\nPeople involved: %s";
            case "message-enrichment" -> "Analyze this message: \"%s\" with context: %s";
            case "participant-analysis" -> "Analyze participant %s with %d messages: %s";
            case "urgency-assessment" -> "Assess urgency of: %s";
            case "topic-generation" -> "Generate topic for: %s";
            case "entity-extraction" -> "Extract entities from: \"%s\"";
            case "sentiment-analysis" -> "Analyze sentiment of: \"%s\"";
            default -> "Analyze: %s";
        };
    }
}
