package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.dto.*;
import com.lucid.automation.airouting.model.AITaskType;
import com.lucid.automation.airouting.model.message.AIMessage;
import com.lucid.automation.airouting.provider.AIProvider;
import com.lucid.automation.airouting.provider.AIProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 * Service for consuming AI processing requests from Kafka and processing them
 */
@Service
public class AIMessageConsumerService {
    
    private static final Logger logger = LoggerFactory.getLogger(AIMessageConsumerService.class);
    
    private final AIProviderFactory providerFactory;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    
    public AIMessageConsumerService(AIProviderFactory providerFactory,
                                  KafkaTemplate<String, Object> kafkaTemplate) {
        this.providerFactory = providerFactory;
        this.kafkaTemplate = kafkaTemplate;
    }
    
    /**
     * Consume categorization requests
     */
    @KafkaListener(topics = "${kafka.topics.ai-categorize:ai-categorize}")
    public void handleCategorizationRequest(@Payload AIMessage message,
                                          @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                          Acknowledgment acknowledgment) {
        logger.info("Received categorization request: messageId={}, tenantId={}", 
                   message.getMessageId(), message.getTenantId());
        
        try {
            AIProvider provider = providerFactory.getProvider(message.getPreferredProvider());
            CategoryResult result = provider.categorize(message.getContent());
            
            // Send response back to reply topic
            sendResponse(message, result, "categorization");
            
            acknowledgment.acknowledge();
            logger.info("Successfully processed categorization request: messageId={}", message.getMessageId());
            
        } catch (Exception e) {
            logger.error("Failed to process categorization request: messageId={}, error={}", 
                        message.getMessageId(), e.getMessage(), e);
            sendErrorResponse(message, "categorization", e.getMessage());
            acknowledgment.acknowledge(); // Acknowledge to avoid reprocessing
        }
    }
    
    /**
     * Consume summarization requests
     */
    @KafkaListener(topics = "${kafka.topics.ai-summarize:ai-summarize}")
    public void handleSummarizationRequest(@Payload AIMessage message,
                                         @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                         Acknowledgment acknowledgment) {
        logger.info("Received summarization request: messageId={}, tenantId={}", 
                   message.getMessageId(), message.getTenantId());
        
        try {
            AIProvider provider = providerFactory.getProvider(message.getPreferredProvider());
            SummaryResult result = provider.summarize(message.getContent());
            
            // Send response back to reply topic
            sendResponse(message, result, "summarization");
            
            acknowledgment.acknowledge();
            logger.info("Successfully processed summarization request: messageId={}", message.getMessageId());
            
        } catch (Exception e) {
            logger.error("Failed to process summarization request: messageId={}, error={}", 
                        message.getMessageId(), e.getMessage(), e);
            sendErrorResponse(message, "summarization", e.getMessage());
            acknowledgment.acknowledge(); // Acknowledge to avoid reprocessing
        }
    }
    
    /**
     * Consume enrichment requests (conversation, message, participant analysis, etc.)
     */
    @KafkaListener(topics = "${kafka.topics.ai-enrich:ai-enrich}")
    public void handleEnrichmentRequest(@Payload AIMessage message,
                                      @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                      Acknowledgment acknowledgment) {
        logger.info("Received enrichment request: messageId={}, taskType={}, tenantId={}", 
                   message.getMessageId(), message.getTaskType(), message.getTenantId());
        
        try {
            AIProvider provider = providerFactory.getProvider(message.getPreferredProvider());
            Object result = processEnrichmentTask(provider, message);
            
            // Send response back to reply topic
            sendResponse(message, result, message.getTaskType().toString().toLowerCase());
            
            acknowledgment.acknowledge();
            logger.info("Successfully processed enrichment request: messageId={}, taskType={}", 
                       message.getMessageId(), message.getTaskType());
            
        } catch (Exception e) {
            logger.error("Failed to process enrichment request: messageId={}, taskType={}, error={}", 
                        message.getMessageId(), message.getTaskType(), e.getMessage(), e);
            sendErrorResponse(message, message.getTaskType().toString().toLowerCase(), e.getMessage());
            acknowledgment.acknowledge(); // Acknowledge to avoid reprocessing
        }
    }
    
    /**
     * Process different types of enrichment tasks
     */
    private Object processEnrichmentTask(AIProvider provider, AIMessage message) {
        AITaskType taskType = message.getTaskType();
        
        return switch (taskType) {
            case ENRICH_CONVERSATION -> {
                if (message.getMessages() == null || message.getMessages().isEmpty()) {
                    throw new IllegalArgumentException("Messages are required for conversation enrichment");
                }
                yield provider.enrichConversation(message.getMessages(), 
                                                 convertToSlackParticipants(message), 
                                                 null); // availableCategories can be extracted from context if needed
            }
            case ENRICH_MESSAGE -> {
                yield provider.enrichMessage(message.getContent(), message.getContext());
            }
            case ANALYZE_PARTICIPANT -> {
                if (message.getParticipants() == null || message.getParticipants().isEmpty()) {
                    throw new IllegalArgumentException("Participants are required for participant analysis");
                }
                // Assuming we analyze the first participant for now
                var participant = convertToSlackParticipant(message.getParticipants().get(0));
                yield provider.analyzeParticipant(participant, message.getMessages());
            }
            case ASSESS_URGENCY -> {
                if (message.getMessages() == null || message.getMessages().isEmpty()) {
                    throw new IllegalArgumentException("Messages are required for urgency assessment");
                }
                yield provider.assessUrgency(message.getMessages());
            }
            case GENERATE_TOPIC -> {
                if (message.getMessages() == null || message.getMessages().isEmpty()) {
                    throw new IllegalArgumentException("Messages are required for topic generation");
                }
                yield provider.generateTopic(message.getMessages());
            }
            case EXTRACT_ENTITIES -> {
                yield provider.extractEntities(message.getContent());
            }
            case SENTIMENT_ANALYSIS -> {
                yield provider.analyzeSentiment(message.getContent());
            }
            default -> throw new IllegalArgumentException("Unsupported enrichment task type: " + taskType);
        };
    }
    
    /**
     * Send successful response back to the reply topic
     */
    private void sendResponse(AIMessage originalMessage, Object result, String taskType) {
        if (originalMessage.getReplyTopic() == null || originalMessage.getReplyTopic().trim().isEmpty()) {
            logger.warn("No reply topic specified for message: messageId={}", originalMessage.getMessageId());
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
            
            kafkaTemplate.send(originalMessage.getReplyTopic(), response);
            logger.info("Sent response to reply topic: messageId={}, replyTopic={}", 
                       originalMessage.getMessageId(), originalMessage.getReplyTopic());
            
        } catch (Exception e) {
            logger.error("Failed to send response to reply topic: messageId={}, replyTopic={}, error={}", 
                        originalMessage.getMessageId(), originalMessage.getReplyTopic(), e.getMessage(), e);
        }
    }
    
    /**
     * Send error response back to the reply topic
     */
    private void sendErrorResponse(AIMessage originalMessage, String taskType, String errorMessage) {
        if (originalMessage.getReplyTopic() == null || originalMessage.getReplyTopic().trim().isEmpty()) {
            logger.warn("No reply topic specified for error response: messageId={}", originalMessage.getMessageId());
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
            
            kafkaTemplate.send(originalMessage.getReplyTopic(), response);
            logger.info("Sent error response to reply topic: messageId={}, replyTopic={}", 
                       originalMessage.getMessageId(), originalMessage.getReplyTopic());
            
        } catch (Exception e) {
            logger.error("Failed to send error response to reply topic: messageId={}, replyTopic={}, error={}", 
                        originalMessage.getMessageId(), originalMessage.getReplyTopic(), e.getMessage(), e);
        }
    }
    
    // Helper methods for data conversion
    private java.util.List<com.lucid.automation.airouting.model.SlackParticipant> convertToSlackParticipants(AIMessage message) {
        if (message.getParticipants() == null) {
            return java.util.Collections.emptyList();
        }
        
        return message.getParticipants().stream()
                .map(this::convertToSlackParticipant)
                .collect(java.util.stream.Collectors.toList());
    }
    
    private com.lucid.automation.airouting.model.SlackParticipant convertToSlackParticipant(
            com.lucid.automation.airouting.model.message.SlackParticipantData participantData) {
        // Convert SlackParticipantData to SlackParticipant
        com.lucid.automation.airouting.model.SlackParticipant participant = 
            new com.lucid.automation.airouting.model.SlackParticipant();
        participant.setId(participantData.getId());
        participant.setName(participantData.getName());
        participant.setDisplayName(participantData.getName()); // Use name as display name if not available
        participant.setEmail(participantData.getEmail());
        participant.setRole(participantData.getRole());
        // Set default values for fields not available in SlackParticipantData
        participant.setBot(false);
        participant.setActive(true);
        participant.setDeleted(false);
        return participant;
    }
}