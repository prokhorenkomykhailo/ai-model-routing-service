package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.model.AITaskType;
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
 * Service for consuming AI processing requests from Kafka and processing them
 */
@Service
public class AIMessageConsumerService {
    
    private static final Logger logger = LoggerFactory.getLogger(AIMessageConsumerService.class);
    
    private final AIProviderFactory providerFactory;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    
    @Value("${kafka.topics.ai-responses:ai.responses.queue}")
    private String aiResponsesTopic;
    
    public AIMessageConsumerService(AIProviderFactory providerFactory,
                                  KafkaTemplate<String, Object> kafkaTemplate) {
        this.providerFactory = providerFactory;
        this.kafkaTemplate = kafkaTemplate;
        
        // Log the guarantee about ai-enrich processing
        logger.info("=== AI-ENRICH PROCESSING GUARANTEE ===");
        logger.info("AIMessageConsumerService initialized");
        logger.info("GUARANTEE: ai-enrich topic will ALWAYS be processed from the beginning");
        logger.info("Mechanisms ensuring this:");
        logger.info("1. aiMessageConsumerFactory with auto-offset-reset=earliest");
        logger.info("2. KafkaOffsetResetService resets offsets on startup");
        logger.info("3. Dedicated consumer group: {}-ai-enrich", "ai-routing-service-group");
        logger.info("======================================");
    }

    /**
     * Consume enrichment requests (conversation, message, participant analysis, etc.)
     * GUARANTEE: This consumer ALWAYS processes ai-enrich topic from the beginning
     * due to the dedicated aiMessageConsumerFactory configuration with auto-offset-reset=earliest
     * and KafkaOffsetResetService that resets offsets on startup.
     */
    @KafkaListener(topics = "${kafka.topics.ai-enrich:ai-enrich}", 
                   containerFactory = "aiMessageListenerContainerFactory")
    public void handleEnrichmentRequest(@Payload AIMessage messageRequest,
                                      @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                      Acknowledgment acknowledgment) {
        // Early null check
        if (messageRequest == null) {
            logger.error("=== HANDLE-ENRICHMENT-ERROR === Received NULL message from topic: {}", topic);
            acknowledgment.acknowledge();
            return;
        }
                   
        logger.info("[X] Received enrichment request: messageId={}, taskType={}, tenantId={}", 
                   messageRequest.getMessageId(), messageRequest.getTaskType(), messageRequest.getTenantId());
        
        // Set default replyTopic if not specified
        if (messageRequest.getReplyTopic() == null || messageRequest.getReplyTopic().trim().isEmpty()) {
            logger.info("No reply topic specified for messageId={}, setting default ai-responses topic", 
                       messageRequest.getMessageId());
            messageRequest.setReplyTopic(aiResponsesTopic);
        }
        
        try {            
            // Handle null or empty preferred provider
            String preferredProvider = messageRequest.getPreferredProvider();
            if (preferredProvider == null || preferredProvider.trim().isEmpty()) {
                logger.warn("No preferred provider specified for messageId={}, using default provider", messageRequest.getMessageId());
                preferredProvider = null; // This will trigger default provider selection
            }
            
            AIProvider provider = preferredProvider != null ? 
                providerFactory.getProvider(preferredProvider) : 
                providerFactory.getDefaultProvider();
                
            if (provider == null) {
                throw new RuntimeException("No AI provider available for processing enrichment request");
            }
                        
            Object result = processEnrichmentTask(provider, messageRequest);
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
        
        return switch (taskType) {
            case ENRICH_CONVERSATION -> {
                if (message.getMessages() == null || message.getMessages().isEmpty()) {
                    throw new IllegalArgumentException("Messages are required for conversation enrichment");
                }

                var participants = convertToSlackParticipants(message);
                yield provider.enrichConversation(message.getMessages(), participants, null);
            }
            case ENRICH_MESSAGE -> {
                logger.info("PROCESS-TASK-DEBUG: Processing ENRICH_MESSAGE for messageId={}", message.getMessageId());
                String content = message.getContent();
                Map<String, Object> context = message.getContext(); 
                yield provider.enrichMessage(content, context);
            }
            case ANALYZE_PARTICIPANT -> {
                logger.debug("Processing ANALYZE_PARTICIPANT for messageId={}", message.getMessageId());
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
            if (aiResponsesTopic.equals(replyTopic)) {
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

                com.lucid.automation.airouting.model.SlackParticipant participant = 
            new com.lucid.automation.airouting.model.SlackParticipant();
        participant.setId(participantData.getId());
        participant.setName(participantData.getName());
        participant.setDisplayName(participantData.getName()); // Use name as display name if not available
        participant.setEmail(participantData.getEmail());
        participant.setRole(participantData.getRole());
        participant.setImageUrl(participantData.getImageUrl());
        participant.setBot(false);
        participant.setActive(true);
        participant.setDeleted(false);
        return participant;
    }
}