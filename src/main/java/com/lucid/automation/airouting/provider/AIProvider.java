package com.lucid.automation.airouting.provider;

import com.lucid.automation.airouting.model.message.AIMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public abstract class AIProvider {
    
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
     * @return String response from the AI provider
     */
    public abstract String processTextQuery(String query);
    
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
            Map<String, Object> tokenData = new HashMap<>();
            tokenData.put("providerId", getProviderId());
            tokenData.put("operation", operation);
            tokenData.put("userId", userId);
            tokenData.put("tenantId", tenantId);
            tokenData.put("inputTokens", inputTokens);
            tokenData.put("outputTokens", outputTokens);
            tokenData.put("totalTokens", totalTokens);
            tokenData.put("timestamp", Instant.now().toString());
            
            if (cost != null) {
                tokenData.put("cost", cost);
            }
            
            if (metadata != null) {
                tokenData.put("metadata", metadata);
            }
            
            kafkaTemplate.send(tokenConsumptionTopic, tokenData);
            
            logger.info("Token consumption sent: provider={}, operation={}, userId={}, tenantId={}, totalTokens={}, cost={}", 
                       getProviderId(), operation, userId, tenantId, totalTokens, cost);
            
        } catch (Exception e) {
            logger.error("Failed to send token consumption data to Kafka: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Convenience method to send token consumption data without cost
     */
    protected void sendTokenConsumption(String operation, int inputTokens, int outputTokens, int totalTokens) {
        sendTokenConsumption(operation, "unknown", "unknown", inputTokens, outputTokens, totalTokens, null, null);
    }
    
    /**
     * Convenience method to send token consumption data with metadata
     */
    protected void sendTokenConsumption(String operation, int inputTokens, int outputTokens, 
                                      int totalTokens, Map<String, Object> metadata) {
        sendTokenConsumption(operation, "unknown", "unknown", inputTokens, outputTokens, totalTokens, null, metadata);
    }
}
