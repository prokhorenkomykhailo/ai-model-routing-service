package com.lucid.automation.airouting.provider.impl;

import com.lucid.automation.airouting.provider.AIProvider;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.message.AIMessage;
import com.lucid.automation.airouting.exception.TokenQuotaExhaustedException;
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
    @Value("${ai.providers.gemini.model:gemini-2.5-flash}")
    private String model;

    @Value("${app.tenant.default-id:default}")
    private String defaultTenantId;

    @Value("${app.tenant.default-schema:public}")
    private String defaultTenantSchema;

    private final Client geminiClient;
    private final ObjectMapper objectMapper;
    private double lastConfidence = 0.0;
    private final boolean isClientAvailable;

    public GeminiProvider(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
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

        // Validate tenant ID using centralized method
        validateTenantId(tenantId, "conversation-enrichment");

        // Use default values if not provided
        if (deemergeUserId == null || deemergeUserId.trim().isEmpty()) {
            deemergeUserId = "unknown";
        }
        if (deemergeUserName == null || deemergeUserName.trim().isEmpty()) {
            deemergeUserName = "Unknown";
        }

        logger.info("📊 GEMINI-ENRICH [{}]: 🏢 Tenant: {}, 👤 User: {}, 📨 Processing {} messages",
                   debugId, tenantId, deemergeUserName, messages != null ? messages.size() : 0);

        // Calculate message statistics
        if (messages != null && !messages.isEmpty()) {
            long uniqueUsers = messages.stream().map(SlackMessage::getUniqueUserId).distinct().count();
            long uniqueChannels = messages.stream().map(SlackMessage::getChannelId).distinct().count();
            logger.info("📈 GEMINI-ENRICH [{}]: Stats | 🏷️ {} unique users, 📺 {} channels",
                       debugId, uniqueUsers, uniqueChannels);
        }        try {
            // Input validation
            if (messages == null || messages.isEmpty()) {
                logger.warn("⚠️ GEMINI-ENRICH [{}]: No messages provided, returning default enrichment", debugId);

                // return getDefaultConversationEnrichment();
                return Map.of(
                    "response", "No messages provided for enrichment",
                    "request", messages
                );
            }

            if (!isClientAvailable) {
                logger.warn("⚠️ GEMINI-ENRICH [{}]: Gemini client not available, returning default enrichment", debugId);
                return Map.of(
                    "response", "Gemini client not available",
                    "request", messages
                );
            }

            logger.info("🔄 GEMINI-ENRICH [{}]: Formatting conversation for analysis", debugId);
            String conversationText = formatConversationForAnalysis(messages);
            String prompt = buildConversationEnrichmentPrompt(conversationText, deemergeUserName);

            // Log prompt statistics instead of full prompt content
            logger.info("📝 GEMINI-ENRICH [{}]: Built enrichment prompt | 📏 {} characters",
                       debugId, prompt != null ? prompt.length() : 0);

            String response = callGeminiAPI(prompt, "conversation-enrichment", debugId, deemergeUserId, tenantId);

            logger.info("✅ GEMINI-ENRICH [{}]: Successfully enriched conversation | 📏 Response: {} chars",
                       debugId, response != null ? response.length() : 0);
            return Map.of(
                "response", response,
                "request", messages
            );
        } catch (IllegalArgumentException e) {
            logger.error("❌ GEMINI-ENRICH [{}]: Invalid input for conversation enrichment: {}", debugId, e.getMessage(), e);
            return Map.of(
                "response", "Invalid input for conversation enrichment: " + e.getMessage(),
                "request", messages
            );
        } catch (RuntimeException e) {
            logger.error("🚨 GEMINI-ENRICH [{}]: API error during conversation enrichment: {}", debugId, e.getMessage(), e);
            return Map.of(
                "response", "API error during conversation enrichment: " + e.getMessage(),
                "request", messages
            );
        } catch (Exception e) {
            logger.error("🚨 GEMINI-ENRICH [{}]: Unexpected error during conversation enrichment: {}", debugId, e.getMessage(), e);
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

    @Override
    public String processTextQuery(String query, String userId, String tenantId) {
        String debugId = "TEXT-QUERY-" + System.currentTimeMillis();
        logger.info("GEMINI-TEXT [{}]: Processing text query: {}", debugId, query != null ? query.substring(0, Math.min(query.length(), 100)) + "..." : "null");

        try {
            // Input validation
            if (query == null || query.trim().isEmpty()) {
                logger.warn("GEMINI-TEXT [{}]: Empty query provided", debugId);
                return "Empty query provided";
            }

            if (!isClientAvailable) {
                logger.warn("GEMINI-TEXT [{}]: Gemini client not available", debugId);
                return "Gemini client not available";
            }

            // Call Gemini API with provided user/tenant context
            String response = callGeminiAPI(query, "text-query", debugId, userId, tenantId);

            logger.info("GEMINI-TEXT [{}]: Successfully processed text query", debugId);
            return response;

        } catch (Exception e) {
            logger.error("GEMINI-TEXT [{}]: Error processing text query: {}", debugId, e.getMessage(), e);
            return "Error processing query: " + e.getMessage();
        }
    }

    // Private helper methods
    private String callGeminiAPI(String prompt, String operation, String debugId, String deemergeUserId, String tenantId) {
        try {
            if (geminiClient == null) {
                logger.error("🚨 GEMINI-API [{}]: Client is not available - API key not configured", debugId);
                throw new RuntimeException("Gemini client is not available - API key not configured");
            }

            long startTime = System.currentTimeMillis();

            // check if tokens are available for the tenant
            if (!isTokenAvailableForTenant(tenantId)) {
                logger.warn("⚠️ GEMINI-API [{}]: No tokens available for tenant {}, cannot process operation: {}", debugId, tenantId, operation);
                throw new TokenQuotaExhaustedException(tenantId, getProviderId());
            }

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

            logger.info("✅ GEMINI-API [{}]: {} operation completed | ⏱️ {}ms | 📊 Input: {} tokens, Output: {} tokens | 📏 Response: {} chars",
                       debugId, operation, duration, inputTokens, outputTokens, outputText != null ? outputText.length() : 0);

            if (outputText == null || outputText.trim().isEmpty()) {
                logger.warn("⚠️ GEMINI-API [{}]: Received empty or null response from Gemini API", debugId);
                throw new RuntimeException("Received empty response from Gemini API");
            }
            return outputText;
        } catch (Exception e) {
            logger.error("🚨 GEMINI-API [{}]: Error calling Gemini API for {} operation: {}", debugId, operation, e.getMessage(), e);
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

            logger.info("📊 Token usage tracked: 🔧 Operation: {}, 👤 User: {}, 🏢 Tenant: {}, 📊 Tokens: {}+{}={}",
                       operation, deemergeUserId, tenantId, inputTokens, outputTokens, totalTokens);

        } catch (Exception e) {
            logger.warn("⚠️ Failed to track token usage for operation {}: {}", operation, e.getMessage());
        }
    }

}
