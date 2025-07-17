package com.lucid.automation.airouting.consumer;

import com.lucid.automation.airouting.model.AITaskType;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.message.AIMessage;
import com.lucid.automation.airouting.provider.AIProvider;
import com.lucid.automation.airouting.provider.AIProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;


/**
 * Consumer service for processing AI requests from Kafka
 * 
 * @author AI Assistant
 */
@Service
public class AIMessageConsumer {
    
    private static final Logger logger = LoggerFactory.getLogger(AIMessageConsumer.class);
    
    private final AIProviderFactory providerFactory;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    
    @Value("${kafka.topics.pre-ai-responses:pre.ai.responses.queue}")
    private String preAiResponsesTopic;
    
    public AIMessageConsumer(AIProviderFactory providerFactory, 
                                  KafkaTemplate<String, Object> kafkaTemplate) {
        this.providerFactory = providerFactory;
        this.kafkaTemplate = kafkaTemplate;
        
        // Log the configuration about ai-enrich processing
        logger.info("=== AI-ENRICH PROCESSING CONFIGURATION ===");
        logger.info("AIMessageConsumer initialized");
        logger.info("CONFIGURATION: ai-enrich topic will process from LATEST offset");
        logger.info("Mechanisms ensuring this:");
        logger.info("1. aiMessageConsumerFactory with auto-offset-reset=latest");
        logger.info("2. Dedicated consumer group: {}-ai-enrich", "ai-routing-service-group");
        logger.info("3. Only NEW messages will be processed, historical messages are SKIPPED");
        logger.info("==========================================");
    }

    /**
     * Consume enrichment requests (conversation, message, participant analysis, etc.)
     * CONFIGURATION: This consumer processes ai-enrich topic from the LATEST offset
     * due to the dedicated aiMessageConsumerFactory configuration with auto-offset-reset=latest.
     * Only NEW messages will be processed, historical messages are SKIPPED.
     */
    @KafkaListener(topics = "${kafka.topics.ai-enrich:ai-enrich}", containerFactory = "aiMessageListenerContainerFactory")
    public void handleEnrichmentRequest(@Payload AIMessage messageRequest,
                                      @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                      Acknowledgment acknowledgment) {
        if (messageRequest == null) {
            logger.error("=== HANDLE-ENRICHMENT-ERROR === Received NULL message from topic: {}", topic);
            acknowledgment.acknowledge();
            return;
        }

        if (messageRequest.getReplyTopic() == null || messageRequest.getReplyTopic().trim().isEmpty()) {
            logger.info("No reply topic specified for messageId={}, setting default ai-responses topic", messageRequest.getMessageId());
            messageRequest.setReplyTopic(preAiResponsesTopic);
        }

        try {            
            // Handle null or empty preferred provider
            String preferredProvider = messageRequest.getPreferredProvider();
            if (preferredProvider == null || preferredProvider.trim().isEmpty()) {
                logger.warn("No preferred provider specified for messageId={}, using default provider", messageRequest.getMessageId());
                preferredProvider = null; // This will trigger default provider selection
            }

            AIProvider provider = preferredProvider != null ? providerFactory.getProvider(preferredProvider) : providerFactory.getDefaultProvider();

            if (provider == null) {
                throw new RuntimeException("No AI provider available for processing enrichment request");
            }
                        
            Object result = processEnrichmentTask(provider, messageRequest); // String
            sendResponse(messageRequest, result, messageRequest.getTaskType().toString().toLowerCase());
            acknowledgment.acknowledge();            
        } catch (Exception e) {
            e.printStackTrace(); // Print full stack trace to console
            sendErrorResponse(messageRequest, messageRequest.getTaskType().toString().toLowerCase(), e.getMessage());
            acknowledgment.acknowledge(); // Acknowledge to avoid reprocessing
        }
    }
    
    private Object processEnrichmentTask(AIProvider provider, AIMessage message) {
        AITaskType taskType = message.getTaskType();
        if (taskType == null) {
            throw new IllegalArgumentException("Task type is required for enrichment processing");
        }

        return switch (taskType) {
            case ENRICH_CONVERSATION -> {
                if (message.getMessages() == null || message.getMessages().isEmpty()) {
                    throw new IllegalArgumentException("Messages are required for conversation enrichment");
                }
                yield provider.enrichConversation(message);
            }
            default -> throw new IllegalArgumentException("Unsupported enrichment task type: " + taskType);
        };
    }
    
    private void sendResponse(AIMessage originalMessage, Object result, String taskType) {
        String replyTopic = originalMessage.getReplyTopic();
        if (replyTopic == null || replyTopic.trim().isEmpty()) {
            logger.error("CRITICAL: Reply topic is null/empty even after setting default! messageId={}", originalMessage.getMessageId());
            return;
        }

        try {
            Map<String, Object> response = new HashMap<>();
            response.put("messageId", originalMessage.getMessageId());
            response.put("correlationId", originalMessage.getCorrelationId());
            response.put("taskType", taskType);
            response.put("status", "success");
            response.put("result", result);
            response.put("processedAt", LocalDateTime.now());
            response.put("tenantId", originalMessage.getTenantId());
            response.put("tenantSchema", originalMessage.getTenantSchema());
            response.put("userId", originalMessage.getUserId());
            Map<String, Object> context = originalMessage.getContext();
            Object deemergeUserIdObj = context != null ? context.get("deemergeUserId") : null;
            Object deemergeUserNameObj = context != null ? context.get("deemergeUserName") : null;
            response.put("deemergeUserId", deemergeUserIdObj != null ? deemergeUserIdObj.toString() : "NoId");
            response.put("deemergeUserName", deemergeUserNameObj != null ? deemergeUserNameObj.toString() : "NoUser");
            response.put("teamId", context != null && context.get("teamId") != null ? context.get("teamId").toString() : "");
            
            kafkaTemplate.send(replyTopic, response);
        } catch (Exception e) {
            logger.error("Failed to send response to reply topic: messageId={}, replyTopic={}, error={}", originalMessage.getMessageId(), replyTopic, e.getMessage(), e);
        }
    }
    
    private void sendErrorResponse(AIMessage originalMessage, String taskType, String errorMessage) {
        String replyTopic = originalMessage.getReplyTopic();
        
        // The replyTopic should always be set by now (in handleEnrichmentRequest)
        if (replyTopic == null || replyTopic.trim().isEmpty()) {
            logger.error("CRITICAL: Reply topic is null/empty even after setting default for error response! messageId={}", 
                        originalMessage.getMessageId());
            return;
        }
        
        try {
            Map<String, Object> response = new HashMap<>();
            response.put("messageId", originalMessage.getMessageId());
            response.put("correlationId", originalMessage.getCorrelationId());
            response.put("taskType", taskType);
            response.put("status", "error");
            response.put("error", errorMessage);
            response.put("processedAt", LocalDateTime.now());
            response.put("tenantId", originalMessage.getTenantId());
            response.put("tenantSchema", originalMessage.getTenantSchema());
            response.put("userId", originalMessage.getUserId());
            
            kafkaTemplate.send(replyTopic, response);
            
            // Log whether this was sent to the default ai-responses topic
            if (preAiResponsesTopic.equals(replyTopic)) {
                logger.info("ERROR: Sent error response to ai-responses topic: messageId={}, topic={}", 
                           originalMessage.getMessageId(), replyTopic);
            } else {
                logger.info("Sent error response to reply topic: messageId={}, replyTopic={}", 
                           originalMessage.getMessageId(), replyTopic);
            }
            
        } catch (Exception e) {
            logger.error("Failed to send error response to reply topic: messageId={}, replyTopic={}, error={}", 
                        originalMessage.getMessageId(), replyTopic, e.getMessage(), e);
        }
    }
}