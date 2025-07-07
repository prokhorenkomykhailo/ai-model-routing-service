package com.lucid.automation.airouting.provider.impl;

import com.lucid.automation.airouting.provider.AIProvider;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.message.AIMessage;
import com.lucid.automation.airouting.util.PromptLoader;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.genai.Client;
import com.google.genai.types.CountTokensResponse;
import com.google.genai.types.GenerateContentResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Component("geminiProvider")
public class GeminiProvider extends AIProvider {
    
    private static final Logger logger = LoggerFactory.getLogger(GeminiProvider.class);
    private static final String MESSAGE_PLACEHOLDER = "##messages##";
    
    @Value("${ai.providers.gemini.api-key:}")
    private String apiKey;
    
    @Value("${ai.providers.gemini.endpoint:https://generativelanguage.googleapis.com/v1/models}")
    private String apiEndpoint;
    
    // @Value("${ai.providers.gemini.model:gemini-2.0-flash}")
    @Value("${ai.providers.gemini.model:gemini-2.5-pro}")
    private String model;
    
    @Value("${app.tenant.default-id:default}")
    private String defaultTenantId;
    
    @Value("${app.tenant.default-schema:public}")
    private String defaultTenantSchema;
    
    private final Client geminiClient;
    private final ObjectMapper objectMapper;
    private final PromptLoader promptLoader;
    private double lastConfidence = 0.0;
    private final boolean isClientAvailable;
    
    public GeminiProvider(ObjectMapper objectMapper, 
                         PromptLoader promptLoader) {
        this.objectMapper = objectMapper;
        this.promptLoader = promptLoader;
        // Try to initialize the client, but handle gracefully if API key is not available
        Client tempClient = null;
        boolean clientAvailable = false;
        
        try {
            // Check if GOOGLE_API_KEY environment variable is set before initializing client
            String googleApiKey = System.getenv("GOOGLE_API_KEY");
            if (googleApiKey != null && !googleApiKey.trim().isEmpty()) {
                tempClient = new Client();
                clientAvailable = true;
                logger.info("Gemini client initialized successfully");
            } else {
                logger.warn("GOOGLE_API_KEY not set, Gemini provider will be unavailable");
            }
        } catch (Exception e) {
            logger.warn("Failed to initialize Gemini client: {}", e.getMessage());
        }
        
        this.geminiClient = tempClient;
        this.isClientAvailable = clientAvailable;
    }
    
    @Override
    public Map<String, Object> enrichConversation(AIMessage request) {
        String debugId = "ENRICH-CONV-" + System.currentTimeMillis();
        logger.info("GEMINI-ENRICH [{}]: Starting conversation enrichment", debugId);
        List<SlackMessage> messages = request.getMessages();
        Map<String, Object> context = request.getContext();
        
        // Extract user and tenant information from context
        String deemergeUserName = (String) context.get("deemergeUserName");
        String deemergeUserId = (String) context.get("deemergeUserId");
        String tenantId = (String) context.get("tenantId");
        
        // Use default values if not provided
        if (deemergeUserId == null || deemergeUserId.trim().isEmpty()) {
            deemergeUserId = "unknown";
        }
        if (tenantId == null || tenantId.trim().isEmpty()) {
            tenantId = "unknown";
        }
        if (deemergeUserName == null || deemergeUserName.trim().isEmpty()) {
            deemergeUserName = "Unknown";
        }
        
        logger.info("[X] GEMINI-ENRICH [{}]: Using userId: {}, tenantId: {}, deemergeUserName: {}", 
                   debugId, deemergeUserId, tenantId, deemergeUserName);
        
        try {
            // Input validation
            if (messages == null || messages.isEmpty()) {
                logger.warn("GEMINI-ENRICH [{}]: No messages provided, returning default enrichment", debugId);
                
                // return getDefaultConversationEnrichment();
                return Map.of(
                    "response", "No messages provided for enrichment",
                    "request", messages
                );
            }
            
            if (!isClientAvailable) {
                logger.warn("GEMINI-ENRICH [{}]: Gemini client not available, returning default enrichment", debugId);
                return Map.of(
                    "response", "Gemini client not available",
                    "request", messages
                );
            }
            
            logger.info("GEMINI-ENRICH [{}]: Formatting conversation for analysis", debugId);
            String conversationText = formatConversationForAnalysis(messages);
            String prompt = buildConversationEnrichmentPrompt(conversationText, deemergeUserName);
            System.out.println("GEMINI-ENRICH [" + debugId + "]: Built conversation enrichment prompt: \n" + prompt);
            String response = callGeminiAPI(prompt, "conversation-enrichment", debugId, deemergeUserId, tenantId);
            return Map.of(
                "response", response,
                "request", messages
            );
        } catch (IllegalArgumentException e) {
            logger.error("GEMINI-ENRICH [{}]: Invalid input for conversation enrichment: {}", debugId, e.getMessage(), e);
            return Map.of(
                "response", "Invalid input for conversation enrichment: " + e.getMessage(),
                "request", messages
            );
        } catch (RuntimeException e) {
            logger.error("GEMINI-ENRICH [{}]: API error during conversation enrichment: {}", debugId, e.getMessage(), e);
            return Map.of(
                "response", "API error during conversation enrichment: " + e.getMessage(),
                "request", messages
            );
        } catch (Exception e) {
            logger.error("GEMINI-ENRICH [{}]: Unexpected error during conversation enrichment: {}", debugId, e.getMessage(), e);
            return Map.of(
                "response", "Unexpected error during conversation enrichment: " + e.getMessage(),
                "request", messages
            );
        }
    }
    
    @Override
    public String getProviderId() {
        return "geminiProvider";
    }
    
    @Override
    public boolean isAvailable() {
        return isClientAvailable;
    }
    
    @Override
    public double getLastConfidence() {
        return lastConfidence;
    }
    
    // Private helper methods
    private String callGeminiAPI(String prompt, String operation, String debugId, String deemergeUserId, String tenantId) {
        try {
            if (geminiClient == null) {
                logger.error("GEMINI-API [{}]: Client is not available - API key not configured", debugId);
                throw new RuntimeException("Gemini client is not available - API key not configured");
            }

            long startTime = System.currentTimeMillis();
            
            // Count input tokens
            CountTokensResponse inputTokenInfo = geminiClient.models.countTokens(model, prompt, null);
            int inputTokens = inputTokenInfo.totalTokens().orElse(0);
            
            // Generate response
            GenerateContentResponse response = geminiClient.models.generateContent(model, prompt, null);
            String outputText = response.text();
            
            // Count output tokens
            CountTokensResponse outputTokenInfo = geminiClient.models.countTokens(model, outputText, null);
            int outputTokens = outputTokenInfo.totalTokens().orElse(0);
            
            long duration = System.currentTimeMillis() - startTime;
            
            // Track token consumption with actual token counts
            trackTokenUsage(operation, inputTokens, outputTokens, deemergeUserId, tenantId);

            logger.info("GEMINI-API [{}]: {} operation completed in {}ms, Input tokens: {}, Output tokens: {}, response: \n\n: {}", 
                       debugId, operation, duration, inputTokens, outputTokens, outputText);
            
            if (outputText == null || outputText.trim().isEmpty()) {
                logger.warn("GEMINI-API [{}]: Received empty or null response from Gemini API", debugId);
                throw new RuntimeException("Received empty response from Gemini API");
            }
            return outputText;            
        } catch (Exception e) {
            logger.error("GEMINI-API [{}]: Error calling Gemini API for {} operation: {}", debugId, operation, e.getMessage(), e);
            throw new RuntimeException("Failed to call Gemini API: " + e.getMessage(), e);
        }
    }

    private void trackTokenUsage(String operation, int inputTokens, int outputTokens, String deemergeUserId, String tenantId) {
        try {
            int totalTokens = inputTokens + outputTokens;
            
            // Create metadata
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("model", model);
            metadata.put("operation", operation);
            metadata.put("actualTokens", true); // Flag to indicate tokens are actual, not estimated

            // Send token consumption data with deemergeUserId and tenantId
            sendTokenConsumption(operation, deemergeUserId, tenantId, inputTokens, outputTokens, totalTokens, null, metadata);

            logger.info("Token usage tracked: operation={}, deemergeUserId={}, tenantId={}, inputTokens={}, outputTokens={}, totalTokens={}",
                       operation, deemergeUserId, tenantId, inputTokens, outputTokens, totalTokens);

        } catch (Exception e) {
            logger.warn("Failed to track token usage for operation {}: {}", operation, e.getMessage());
        }
    }
    
    private String buildConversationEnrichmentPrompt(String conversationText, String deemergeUserName) {
        String template = promptLoader.loadPromptTemplate("conversation-enrichment");
        String formattedPrompt = template.replace("{{current_user}}", deemergeUserName);

        if (formattedPrompt.contains(MESSAGE_PLACEHOLDER)) {
            return formattedPrompt.replace(MESSAGE_PLACEHOLDER, MESSAGE_PLACEHOLDER + "\n" +conversationText);
        } else {
            return formattedPrompt + "\n" + conversationText;
        }
    }

    
    // Utility methods
    private String formatConversationForAnalysis(List<SlackMessage> messages) {
        return messages.stream()
            .map(this::formatMessageForAnalysis)
            .collect(Collectors.joining("\n"));
    }
    
    private String formatMessageForAnalysis(SlackMessage msg) {
        //  {
        //   “role”: “user”,
        //   “content”: “...“,
        //   “author”: “Benoit”,
        //   “timestamp”: ...,
        //   “channel_id”: “...“,
        //   “thread_ts”: ...
        //  },

        try {
            Map<String, Object> messageMap = new LinkedHashMap<>();
            messageMap.put("ROLE", safeString("user"));
            
            // Message content (prioritize content over text)
            String content = msg.getContent();
            if (content == null || content.trim().isEmpty()) {
                content = msg.getText();
            }
            messageMap.put("CONTENT", safeString(content));
            messageMap.put("USER_ID", safeString(msg.getSlackUserId()));
            messageMap.put("AUTHOR", safeString(msg.getDisplayName()));

            messageMap.put("TIMESTAMP", msg.getTimestamp());
            // Channel and thread context
            messageMap.put("CHANNEL_ID", safeString(msg.getChannelId()));

            if (msg.getThreadTs() != null) {
                messageMap.put("THREAD_TS", msg.getThreadTs());
            }            
            return objectMapper.writeValueAsString(messageMap);
        } catch (Exception e) {
            logger.warn("Failed to format message as JSON: {}", e.getMessage());
            return "{}";
        }
    }
    
    private String safeString(String value) {
        return value != null ? value : "N/A";
    }
}
