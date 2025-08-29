package com.lucid.automation.airouting.provider;

import com.lucid.automation.airouting.model.message.AIMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.Map;

import com.lucid.automation.common.dto.TokenConsumptionDTO;
import com.lucid.automation.common.dto.TokenQuotaResponseDTO;
import com.lucid.automation.airouting.service.EnhancedTokenAvailabilityService;

public abstract class AIProvider {

    @Autowired
    private EnhancedTokenAvailabilityService enhancedTokenAvailabilityService;

    private static final Logger logger = LoggerFactory.getLogger(AIProvider.class);

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topics.ai-token-consumption:ai-token-consumption}")
    private String tokenConsumptionTopic;

    /**
     * Enrich an entire conversation with dynamic categories
     *
     * @param messages The AI message containing the list of Slack messages to analyze and context
     * @return Map containing the analysis results
     */
    public abstract Map<String, Object> enrichConversation(AIMessage messages);

    /**
     * Process a simple text query and return a response
     *
     * @param query The text query to process
     * @param userId The user ID associated with the request
     * @param tenantId The tenant ID associated with the request
     * @return String response from the AI provider
     */
    public abstract String processTextQuery(String query, String userId, String tenantId);

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
}
