package com.lucid.automation.airouting.provider;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucid.automation.airouting.exception.InvalidTenantException;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.message.AIMessage;
import com.lucid.automation.airouting.util.TenantValidationUtil;
import com.lucid.automation.airouting.util.PromptLoader;
import com.lucid.automation.airouting.client.AnonymizationClient;
import com.lucid.automation.airouting.dto.anonymization.MaskRequest;
import com.lucid.automation.airouting.dto.anonymization.MaskResponse;
import com.lucid.automation.airouting.dto.anonymization.UnmaskRequest;
import com.lucid.automation.airouting.dto.anonymization.UnmaskResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.lucid.automation.common.dto.TokenConsumptionDTO;
import com.lucid.automation.common.dto.TokenQuotaResponseDTO;
import com.lucid.automation.airouting.service.EnhancedTokenAvailabilityService;

public abstract class AIProvider {

    private static final Logger logger = LoggerFactory.getLogger(AIProvider.class);
    protected static final String MESSAGE_PLACEHOLDER = "##messages##";

    @Autowired
    private EnhancedTokenAvailabilityService enhancedTokenAvailabilityService;

    @Autowired
    protected PromptLoader promptLoader;

    @Autowired
    protected ObjectMapper objectMapper;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired(required = false)
    protected AnonymizationClient anonymizationClient;

    @Value("${kafka.topics.ai-token-consumption:ai-token-consumption}")
    private String tokenConsumptionTopic;

    @Value("${anonymization.service.enabled:false}")
    protected boolean anonymizationEnabled;

    /**
     * Template method for conversation enrichment with PII protection.
     * This method orchestrates the complete flow: validate → mask → enrich → unmask
     *
     * Concrete providers should NOT override this method.
     * Instead, implement {@link #doEnrichConversation(AIMessage, String, String, String)}
     *
     * @param messages The AI message containing the list of Slack messages to analyze and context
     * @return Map containing the analysis results with PII unmasked
     */
    public final Map<String, Object> enrichConversation(AIMessage messages) {
        String debugId = "ENRICH-" + System.currentTimeMillis();
        Map<String, Object> context = messages.getContext();
        String tenantId = (String) context.get("tenantId");
        String deemergeUserId = (String) context.get("deemergeUserId");
        String deemergeUserName = (String) context.get("deemergeUserName");

        try {
            // Step 1: Validate tenant
            validateTenantId(tenantId, "conversation-enrichment");

            // Step 2: Let concrete provider build the prompt and call LLM (with masking inside)
            return doEnrichConversation(messages, tenantId, deemergeUserId, deemergeUserName, debugId);

        } catch (Exception e) {
            logger.error("🚨 {}: [{}] Error in conversation enrichment: {}",
                        getProviderId(), debugId, e.getMessage(), e);
            return Map.of(
                "response", "Error during conversation enrichment: " + e.getMessage(),
                "request", messages
            );
        }
    }

    /**
     * Template method for text query processing with PII protection.
     * This method orchestrates the complete flow: validate → mask → query → unmask
     *
     * Concrete providers should NOT override this method.
     * Instead, implement {@link #doProcessTextQuery(String, String, String, String)}
     *
     * @param query The text query to process
     * @param userId The user ID making the request
     * @param tenantId The tenant ID for the request
     * @return String response from the AI provider with PII unmasked
     */
    public final String processTextQuery(String query, String userId, String tenantId) {
        String debugId = "TEXT-QUERY-" + System.currentTimeMillis();

        try {
            // Step 1: Validate tenant
            validateTenantId(tenantId, "text-query");

            // Step 2: Validate input
            if (query == null || query.trim().isEmpty()) {
                logger.warn("⚠️ {}: [{}] Empty query provided", getProviderId(), debugId);
                return "Empty query provided";
            }

            // Step 3: Mask PII in query
            String maskedQuery = maskPII(query, tenantId, debugId);

            // Step 4: Let concrete provider call LLM
            String maskedResponse = doProcessTextQuery(maskedQuery, userId, tenantId, debugId);

            // Step 5: Unmask PII in response
            return unmaskPII(maskedResponse, tenantId, debugId);

        } catch (Exception e) {
            logger.error("🚨 {}: [{}] Error processing text query: {}",
                        getProviderId(), debugId, e.getMessage(), e);
            return "Error processing query: " + e.getMessage();
        }
    }

    /**
     * Concrete providers must implement this to perform the actual conversation enrichment.
     * This method should build the prompt, mask PII, call the LLM API, and unmask the response.
     *
     * @param messages The AI message with conversation context
     * @param tenantId The tenant ID (already validated)
     * @param deemergeUserId The user ID
     * @param deemergeUserName The user name
     * @param debugId Debug identifier for logging
     * @return Map containing the enrichment results
     */
    protected abstract Map<String, Object> doEnrichConversation(
        AIMessage messages, String tenantId, String deemergeUserId, String deemergeUserName, String debugId);

    /**
     * Concrete providers must implement this to perform the actual text query processing.
     * The query is already masked, and the response will be unmasked by the template method.
     *
     * @param maskedQuery The query with PII already masked
     * @param userId The user ID
     * @param tenantId The tenant ID (already validated)
     * @param debugId Debug identifier for logging
     * @return The response from the LLM (still masked)
     */
    protected abstract String doProcessTextQuery(String maskedQuery, String userId, String tenantId, String debugId);

    /**
     * Get provider identifier
     */
    public abstract String getProviderId();

    /**
     * Check if provider is available
     */
    public abstract boolean isAvailable();

    /**
     * Get last confidence score
     */
    public abstract double getLastConfidence();

    /**
     * Validates tenant ID before processing AI requests.
     * This method provides centralized tenant validation across all AI providers.
     *
     * @param tenantId The tenant ID to validate
     * @param operation The operation being performed (for logging purposes)
     * @throws InvalidTenantException if the tenant ID is invalid
     */
    protected void validateTenantId(String tenantId, String operation) {
        if (TenantValidationUtil.isInvalidTenantId(tenantId)) {
            String reason = TenantValidationUtil.getInvalidTenantIdReason(tenantId);
            logger.warn("⚠️ {}: Rejecting {} operation due to invalid tenant ID [{}]: {}",
                       getProviderId(), operation, tenantId, reason);
            throw new InvalidTenantException(tenantId, reason);
        }

        logger.debug("{}: Tenant ID validation passed for operation: {}, tenant: {}",
                    getProviderId(), operation, tenantId);
    }

    /**
     * Mask PII in text before sending to LLM provider.
     * This method provides centralized PII protection across all AI providers.
     *
     * @param text The text to mask
     * @param tenantId The tenant ID for token vault isolation
     * @param debugId Request ID for logging correlation
     * @return Masked text with PII replaced by placeholders, or original text if masking fails/disabled
     */
    protected String maskPII(String text, String tenantId, String debugId) {
        // Skip masking if feature is disabled or client not available
        if (!anonymizationEnabled || anonymizationClient == null) {
            logger.debug("🔓 {}: [{}] PII masking disabled or client unavailable", getProviderId(), debugId);
            return text;
        }

        try {
            logger.info("🔒 {}: [{}] Masking PII before LLM call | Tenant: {}",
                       getProviderId(), debugId, tenantId);

            MaskRequest request = new MaskRequest();
            request.setText(text);
            request.setConfidenceThreshold(0.5);
            request.setTenantId(tenantId);

            MaskResponse response = anonymizationClient.maskText(request);

            logger.info("✅ {}: [{}] PII masking successful | Entities: {} | Original length: {} | Masked length: {}",
                       getProviderId(), debugId, response.getEntitiesFound(),
                       text.length(), response.getMaskedText().length());

            return response.getMaskedText();
        } catch (Exception e) {
            logger.error("❌ {}: [{}] PII masking failed: {} | Falling back to original text",
                        getProviderId(), debugId, e.getMessage());
            // Fail-open pattern: return original text to avoid blocking LLM calls
            return text;
        }
    }

    /**
     * Unmask PII in LLM response before returning to user.
     * This method provides centralized PII restoration across all AI providers.
     *
     * @param text The text to unmask
     * @param tenantId The tenant ID for token vault access
     * @param debugId Request ID for logging correlation
     * @return Unmasked text with placeholders replaced by original PII, or original text if unmasking fails/disabled
     */
    protected String unmaskPII(String text, String tenantId, String debugId) {
        // Skip unmasking if feature is disabled or client not available
        if (!anonymizationEnabled || anonymizationClient == null) {
            logger.debug("🔓 {}: [{}] PII unmasking disabled or client unavailable", getProviderId(), debugId);
            return text;
        }

        try {
            logger.info("🔓 {}: [{}] Unmasking PII after LLM response | Tenant: {}",
                       getProviderId(), debugId, tenantId);

            UnmaskRequest request = new UnmaskRequest();
            request.setMaskedText(text);
            request.setTenantId(tenantId);
            request.setJustification("AI response re-enrichment for user display");

            UnmaskResponse response = anonymizationClient.unmaskText(request);

            logger.info("✅ {}: [{}] PII unmasking successful | Tokens: {} processed, {} retrieved | Request ID: {}",
                       getProviderId(), debugId, response.getTokensProcessed(),
                       response.getTokensRetrieved(), response.getRequestId());

            return response.getUnmaskedText();
        } catch (Exception e) {
            logger.error("❌ {}: [{}] PII unmasking failed: {} | Returning masked text",
                        getProviderId(), debugId, e.getMessage());
            // Fail-open pattern: return masked text to avoid breaking responses
            // Note: This means PII will remain masked in the response if unmasking fails
            return text;
        }
    }

    /**
     * Send token consumption data to Kafka topic
     *
     * @param operation The operation that consumed tokens (e.g., "enrichConversation")
     * @param userId The user ID associated with the request
     * @param tenantId The tenant ID associated with the request
     * @param inputTokens Number of input tokens consumed
     * @param outputTokens Number of output tokens generated
     * @param totalTokens Total tokens consumed
     * @param cost Optional cost of the operation
     * @param metadata Additional metadata about the operation
     */
    protected void sendTokenConsumption(String operation, String userId, String tenantId,
                                      int inputTokens, int outputTokens, int totalTokens,
                                      Double cost, Map<String, Object> metadata) {
        try {
            TokenConsumptionDTO tokenData = TokenConsumptionDTO.builder()
                    .providerId(getProviderId())
                    .operation(operation)
                    .userId(userId)
                    .tenantId(tenantId)
                    .inputTokens(inputTokens)
                    .outputTokens(outputTokens)
                    .totalTokens(totalTokens)
                    .timestamp(Instant.now())
                    .cost(cost)
                    .metadata(metadata)
                    .build();
            kafkaTemplate.send(tokenConsumptionTopic, tokenData);
        } catch (Exception e) {
            logger.error("Failed to send token consumption data to Kafka: {}", e.getMessage(), e);
        }
    }

        /**
     * Checks if the tenant has tokens available using quota information.
     * @param tenantId the tenant ID to check
     * @return true if tokens are available, false otherwise
     */
    protected boolean isTokenAvailableForTenant(String tenantId) {
        try {
            //TODO: uncomment to activate
            // TokenQuotaResponseDTO quota = enhancedTokenAvailabilityService.getTokenQuota(tenantId);
            // return quota != null && quota.isAvailable() && quota.getRemainingTokens() > 0;
            return true;
        } catch (Exception e) {
            logger.warn("Failed to check token availability for tenant {}: {}", tenantId, e.getMessage());
            // Conservative fallback - assume tokens are available to avoid blocking operations
            return true;
        }
    }

    /**
     * Checks if the tenant has sufficient tokens for a specific operation.
     * @param tenantId the tenant ID to check
     * @param requiredTokens number of tokens required for the operation
     * @return true if sufficient tokens are available, false otherwise
     */
    protected boolean hasSufficientTokens(String tenantId, long requiredTokens) {
        try {
            return enhancedTokenAvailabilityService.hasSufficientTokens(tenantId, requiredTokens);
        } catch (Exception e) {
            logger.warn("Failed to check token sufficiency for tenant {} (required: {}): {}",
                       tenantId, requiredTokens, e.getMessage());
            // Conservative fallback - assume tokens are sufficient to avoid blocking operations
            return true;
        }
    }

    /**
     * Gets detailed token quota information for the tenant.
     * @param tenantId the tenant ID to check
     * @return TokenQuotaResponseDTO with detailed quota information, null if unavailable
     */
    protected TokenQuotaResponseDTO getTokenQuota(String tenantId) {
        try {
            return enhancedTokenAvailabilityService.getTokenQuota(tenantId);
        } catch (Exception e) {
            logger.warn("Failed to get token quota for tenant {}: {}", tenantId, e.getMessage());
            return null;
        }
    }

    /**
     * Load a prompt template using the injected PromptLoader.
     * This method is available to all AI provider implementations.
     *
     * @param promptName The name of the prompt template to load
     * @return The loaded prompt template string
     */
    protected String loadPromptTemplate(String promptName) {
        return promptLoader.loadPromptTemplate(promptName);
    }

    /**
     * Build conversation enrichment prompt by loading template and replacing placeholders.
     * This method is shared across all AI provider implementations.
     *
     * @param conversationText The formatted conversation text
     * @param deemergeUserName The current user's name
     * @return The formatted prompt with conversation text injected
     */
    protected String buildConversationEnrichmentPrompt(String conversationText, String deemergeUserName) {
        String template = loadPromptTemplate("conversation-enrichment");
        String formattedPrompt = template.replace("{{current_user}}", deemergeUserName);

        if (formattedPrompt.contains(MESSAGE_PLACEHOLDER)) {
            return formattedPrompt.replace(MESSAGE_PLACEHOLDER, MESSAGE_PLACEHOLDER + "\n" + conversationText);
        } else {
            return formattedPrompt + "\n" + conversationText;
        }
    }

    /**
     * Format a list of Slack messages into a string suitable for AI analysis.
     * This method is shared across all AI provider implementations.
     *
     * @param messages List of SlackMessage objects to format
     * @return Formatted conversation string with each message as a JSON line
     */
    protected String formatConversationForAnalysis(List<SlackMessage> messages) {
        return messages.stream()
            .map(this::formatMessageForAnalysis)
            .collect(Collectors.joining("\n"));
    }

    /**
     * Format a single Slack message into JSON representation for AI analysis.
     * This method is shared across all AI provider implementations.
     *
     * @param msg The SlackMessage to format
     * @return JSON string representation of the message
     */
    protected String formatMessageForAnalysis(SlackMessage msg) {
        try {
            Map<String, Object> messageMap = new LinkedHashMap<>();
            messageMap.put("ROLE", safeString("user"));

            // Message content (prioritize content over text)
            String content = msg.getContent();
            if (content == null || content.trim().isEmpty()) {
                content = msg.getText();
            }
            messageMap.put("CONTENT", safeString(content));

            // USER_ID: Use uniqueUserId (platform-agnostic identifier)
            String userId = msg.getUniqueUserId();
            messageMap.put("USER_ID", safeString(userId));

            messageMap.put("AUTHOR", safeString(msg.getDisplayName()));

            messageMap.put("TIMESTAMP", msg.getTimestamp());

            // Channel and thread context
            if (msg.getChannelId() != null) {
                messageMap.put("CHANNEL_ID", safeString(msg.getChannelId()));
            }

            if (msg.getThreadTs() != null) {
                messageMap.put("THREAD_TS", msg.getThreadTs());
            }
            return objectMapper.writeValueAsString(messageMap);
        } catch (Exception e) {
            logger.warn("⚠️ Failed to format message as JSON: {}", e.getMessage());
            return "{}";
        }
    }

    /**
     * Safely convert a string value to a non-null representation.
     * This method is shared across all AI provider implementations.
     *
     * @param value The string value to check
     * @return The original value if not null, "N/A" otherwise
     */
    protected String safeString(String value) {
        return value != null ? value : "N/A";
    }
}
