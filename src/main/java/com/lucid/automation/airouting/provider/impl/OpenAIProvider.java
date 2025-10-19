package com.lucid.automation.airouting.provider.impl;

import com.lucid.automation.airouting.provider.AIProvider;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.message.AIMessage;
import com.lucid.automation.airouting.exception.TokenQuotaExhaustedException;
import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.models.ChatCompletion;
import com.openai.models.ChatCompletionCreateParams;
import com.openai.models.ChatCompletionMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;

@Component("openaiProvider")
public class OpenAIProvider extends AIProvider {

    private static final Logger logger = LoggerFactory.getLogger(OpenAIProvider.class);

    @Value("${ai.providers.openai.api-key:}")
    private String apiKey;

    @Value("${ai.providers.openai.endpoint:https://api.openai.com/v1}")
    private String apiEndpoint;

    @Value("${ai.providers.openai.model:gpt-3.5-turbo}")
    private String model;

    @Value("${ai.providers.openai.timeout:30000}")
    private int timeoutMs;

    @Value("${ai.providers.openai.enabled:true}")
    private boolean enabled;

    @Value("${app.tenant.default-id:default}")
    private String defaultTenantId;

    @Value("${app.tenant.default-schema:public}")
    private String defaultTenantSchema;

    private final OpenAIClient openaiClient;
    private double lastConfidence = 0.0;
    private final boolean isClientAvailable;

    public OpenAIProvider() {
        // Try to initialize the client, but handle gracefully if API key is not available
        OpenAIClient tempClient = null;
        boolean clientAvailable = false;

        try {
            // Check if OPENAI_API_KEY environment variable is set before initializing client
            String openaiApiKey = System.getenv("OPENAI_API_KEY");
            if (openaiApiKey != null && !openaiApiKey.trim().isEmpty()) {
                tempClient = OpenAIOkHttpClient.builder()
                    .apiKey(openaiApiKey)
                    .build();
                clientAvailable = true;
                logger.info("OpenAI client initialized successfully");
            } else {
                logger.warn("OPENAI_API_KEY not set, OpenAI provider will be unavailable");
            }
        } catch (Exception e) {
            logger.warn("Failed to initialize OpenAI client: {}", e.getMessage());
        }

        this.openaiClient = tempClient;
        this.isClientAvailable = clientAvailable;
    }

    @Override
    protected Map<String, Object> doEnrichConversation(
            AIMessage request, String tenantId, String deemergeUserId, String deemergeUserName, String debugId) {

        logger.info("OPENAI-ENRICH [{}]: Starting conversation enrichment", debugId);
        List<SlackMessage> messages = request.getMessages();

        // Use default values if not provided
        if (deemergeUserId == null || deemergeUserId.trim().isEmpty()) {
            deemergeUserId = "unknown";
        }
        if (deemergeUserName == null || deemergeUserName.trim().isEmpty()) {
            deemergeUserName = "Unknown";
        }

        logger.info("[X] OPENAI-ENRICH [{}]: Using userId: {}, tenantId: {}, deemergeUserName: {}",
                   debugId, deemergeUserId, tenantId, deemergeUserName);

        try {
            // Input validation
            if (messages == null || messages.isEmpty()) {
                logger.warn("OPENAI-ENRICH [{}]: No messages provided, returning default enrichment", debugId);
                return Map.of(
                    "response", "No messages provided for enrichment",
                    "request", messages
                );
            }

            if (!isClientAvailable) {
                logger.warn("OPENAI-ENRICH [{}]: OpenAI client not available, returning default enrichment", debugId);
                return Map.of(
                    "response", "OpenAI client not available",
                    "request", messages
                );
            }

            logger.info("OPENAI-ENRICH [{}]: Formatting conversation for analysis", debugId);
            String conversationText = formatConversationForAnalysis(messages);
            String prompt = buildConversationEnrichmentPrompt(conversationText, deemergeUserName);
            System.out.println("OPENAI-ENRICH [" + debugId + "]: Built conversation enrichment prompt: \n" + prompt);

            // Mask PII before sending to OpenAI
            String maskedPrompt = maskPII(prompt, tenantId, debugId);

            String maskedResponse = callOpenAIAPI(maskedPrompt, "conversation-enrichment", debugId, deemergeUserId, tenantId);

            // Unmask PII before returning to user
            String response = unmaskPII(maskedResponse, tenantId, debugId);

            return Map.of(
                "response", response,
                "request", messages
            );
        } catch (IllegalArgumentException e) {
            logger.error("OPENAI-ENRICH [{}]: Invalid input for conversation enrichment: {}", debugId, e.getMessage(), e);
            return Map.of(
                "response", "Invalid input for conversation enrichment: " + e.getMessage(),
                "request", messages
            );
        } catch (RuntimeException e) {
            logger.error("OPENAI-ENRICH [{}]: API error during conversation enrichment: {}", debugId, e.getMessage(), e);
            return Map.of(
                "response", "API error during conversation enrichment: " + e.getMessage(),
                "request", messages
            );
        } catch (Exception e) {
            logger.error("OPENAI-ENRICH [{}]: Unexpected error during conversation enrichment: {}", debugId, e.getMessage(), e);
            return Map.of(
                "response", "Unexpected error during conversation enrichment: " + e.getMessage(),
                "request", messages
            );
        }
    }

    @Override
    protected String doProcessTextQuery(String maskedQuery, String userId, String tenantId, String debugId) {
        logger.info("🔍 OPENAI-TEXT [{}]: Processing text query | 📏 {} chars | 👤 User: {} | 🏢 Tenant: {}",
                   debugId, maskedQuery != null ? maskedQuery.length() : 0,
                   userId != null ? userId : "unknown", tenantId != null ? tenantId : "unknown");

        try {
            if (!isClientAvailable) {
                logger.warn("⚠️ OPENAI-TEXT [{}]: OpenAI client not available", debugId);
                return "OpenAI client not available";
            }

            // Call OpenAI API - query is already masked by template method
            String maskedResponse = callOpenAIAPI(maskedQuery, "text-query", debugId, userId, tenantId);

            logger.info("✅ OPENAI-TEXT [{}]: Successfully processed text query | 📏 Response: {} chars",
                       debugId, maskedResponse != null ? maskedResponse.length() : 0);

            // Return masked response - template method will unmask it
            return maskedResponse;

        } catch (Exception e) {
            logger.error("🚨 OPENAI-TEXT [{}]: Error processing text query: {}", debugId, e.getMessage(), e);
            throw new RuntimeException("Error processing query: " + e.getMessage(), e);
        }
    }

    @Override
    public String getProviderId() {
        return "openaiProvider";
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
    private String callOpenAIAPI(String prompt, String operation, String debugId, String deemergeUserId, String tenantId) {
        try {
            if (!isClientAvailable) {
                logger.error("🚨 OPENAI-API [{}]: Client is not available - API key not configured", debugId);
                throw new RuntimeException("OpenAI client is not available - API key not configured");
            }

            long startTime = System.currentTimeMillis();

            // check if tokens are available for the tenant
            if (!isTokenAvailableForTenant(tenantId)) {
                logger.warn("⚠️ OPENAI-API [{}]: No tokens available for tenant {}, cannot process operation : {}", debugId, tenantId, operation);
                throw new TokenQuotaExhaustedException(tenantId, getProviderId());
            }

            // Build OpenAI request using official client
            ChatCompletionCreateParams params = ChatCompletionCreateParams.builder()
                    .model(model)
                    .temperature(0.7)
                    .addUserMessage(prompt)
                    .build();

            // Call OpenAI API using official client
            ChatCompletion completion = openaiClient.chat().completions().create(params);

            long duration = System.currentTimeMillis() - startTime;

            // Extract response content
            if (completion.choices().isEmpty()) {
                throw new RuntimeException("No choices returned from OpenAI API");
            }

            ChatCompletionMessage message = completion.choices().get(0).message();
            String outputText = message.content().orElse("");

            // Extract token usage if available
            int inputTokens = completion.usage().map(usage -> (int) usage.promptTokens()).orElse(0);
            int outputTokens = completion.usage().map(usage -> (int) usage.completionTokens()).orElse(0);

            // Track token consumption
            trackTokenUsage(operation, inputTokens, outputTokens, deemergeUserId, tenantId);

            logger.info("✅ OPENAI-API [{}]: {} operation completed | ⏱️ {}ms | 📊 Input: {} tokens, Output: {} tokens | 📏 Response: {} chars",
                       debugId, operation, duration, inputTokens, outputTokens, outputText != null ? outputText.length() : 0);

            if (outputText == null || outputText.trim().isEmpty()) {
                logger.warn("⚠️ OPENAI-API [{}]: Received empty or null response from OpenAI API", debugId);
                throw new RuntimeException("Received empty response from OpenAI API");
            }
            return outputText;
        } catch (Exception e) {
            logger.error("🚨 OPENAI-API [{}]: Error calling OpenAI API for {} operation: {}", debugId, operation, e.getMessage(), e);
            throw new RuntimeException("Failed to call OpenAI API: " + e.getMessage(), e);
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
}
