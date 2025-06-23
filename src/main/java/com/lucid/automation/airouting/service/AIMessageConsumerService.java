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
        
        // VERIFICATION: Log that we're ALWAYS processing ai-enrich from the start
        logger.info("=== HANDLE-ENRICHMENT-START === Topic: {}, MessageId: {}, Thread: {}", 
                   topic, messageRequest != null ? messageRequest.getMessageId() : "NULL-MESSAGE", 
                   Thread.currentThread().getName());
        
        // FORCE CONSOLE OUTPUT - This should ALWAYS appear when processing ai-enrich
        System.out.println(">>>>>>> CONSOLE DEBUG: handleEnrichmentRequest called! Topic: " + topic + ", MessageId: " + (messageRequest != null ? messageRequest.getMessageId() : "NULL") + 
                          ", Thread: " + Thread.currentThread().getName());
        System.out.println(">>>>>>> GUARANTEE: ai-enrich topic is ALWAYS processed from the beginning");
        System.out.println(">>>>>>> This is ensured by: 1) auto-offset-reset=earliest, 2) KafkaOffsetResetService");
        
        // Early null check
        if (messageRequest == null) {
            logger.error("=== HANDLE-ENRICHMENT-ERROR === Received NULL message from topic: {}", topic);
            System.out.println(">>>>>>> CONSOLE DEBUG: NULL MESSAGE RECEIVED!");
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
            System.out.println(">>>>>>> CONSOLE DEBUG: Set default replyTopic to: " + aiResponsesTopic + 
                             " for messageId: " + messageRequest.getMessageId());
        }
        
        // CONSOLE RAW MESSAGE DEBUG - ALL FIELDS
        System.out.println("===========================================");
        System.out.println(">>>>>>> RAW AI MESSAGE - ALL FIELDS:");
        System.out.println("===========================================");
        System.out.println("  MessageId: " + messageRequest.getMessageId());
        System.out.println("  TaskType: " + messageRequest.getTaskType());
        System.out.println("  Content: '" + messageRequest.getContent() + "'");
        System.out.println("  ConversationId: " + messageRequest.getConversationId());
        System.out.println("  TenantId: " + messageRequest.getTenantId());
        System.out.println("  TenantSchema: " + messageRequest.getTenantSchema());
        System.out.println("  UserId: " + messageRequest.getUserId());
        System.out.println("  PreferredProvider: " + messageRequest.getPreferredProvider());
        System.out.println("  ReplyTopic: " + messageRequest.getReplyTopic());
        System.out.println("  CorrelationId: " + messageRequest.getCorrelationId());
        System.out.println("  Priority: " + messageRequest.getPriority());
        System.out.println("  RequestedAt: " + messageRequest.getRequestedAt());
        
        // Messages collection
        if (messageRequest.getMessages() != null) {
            System.out.println("  Messages: [" + messageRequest.getMessages().size() + " total]");
            for (int i = 0; i < Math.min(3, messageRequest.getMessages().size()); i++) {
                var msg = messageRequest.getMessages().get(i);
                System.out.println("    Message[" + i + "]:");
                System.out.println("      UserId: " + msg.getUserId());
                System.out.println("      Content: '" + (msg.getContent() != null ? 
                                 msg.getContent().substring(0, Math.min(100, msg.getContent().length())) : "null") + "'");
                System.out.println("      Timestamp: " + msg.getTimestamp());
            }
            if (messageRequest.getMessages().size() > 3) {
                System.out.println("    ... and " + (messageRequest.getMessages().size() - 3) + " more messages");
            }
        } else {
            System.out.println("  Messages: null");
        }
        
        // Participants collection  
        if (messageRequest.getParticipants() != null) {
            System.out.println("  Participants: [" + messageRequest.getParticipants().size() + " total]");
            for (int i = 0; i < Math.min(3, messageRequest.getParticipants().size()); i++) {
                var participant = messageRequest.getParticipants().get(i);
                System.out.println("    Participant[" + i + "]:");
                System.out.println("      Id: " + participant.getId());
                System.out.println("      Name: " + participant.getName());
                System.out.println("      Email: " + participant.getEmail());
            }
        } else {
            System.out.println("  Participants: null");
        }
        
        // Context
        if (messageRequest.getContext() != null) {
            System.out.println("  Context: " + messageRequest.getContext());
        } else {
            System.out.println("  Context: null");
        }
        System.out.println("===========================================");
        System.out.println("  TenantId: " + messageRequest.getTenantId());
        System.out.println("  PreferredProvider: " + messageRequest.getPreferredProvider());
        System.out.println("  Messages count: " + (messageRequest.getMessages() != null ? messageRequest.getMessages().size() : "null"));
        System.out.println("  Participants count: " + (messageRequest.getParticipants() != null ? messageRequest.getParticipants().size() : "null"));
        
        if (messageRequest.getMessages() != null && !messageRequest.getMessages().isEmpty()) {
            System.out.println("  First message content: '" + 
                (messageRequest.getMessages().get(0).getText() != null ? 
                    messageRequest.getMessages().get(0).getText().substring(0, Math.min(100, messageRequest.getMessages().get(0).getContent().length())) : 
                    "null") + "'");
        }
        System.out.println(">>>>>>> END RAW MESSAGE DEBUG");
        
        // Add detailed logging to debug empty/null content issues - ALWAYS at INFO level
        logger.info("ENRICH-INPUT-DEBUG: messageId={}, taskType={}, content='{}', conversationId={}, messagesCount={}, participantsCount={}", 
                messageRequest.getMessageId(), 
                messageRequest.getTaskType(), 
                messageRequest.getContent() != null ? messageRequest.getContent() : "NULL", 
                messageRequest.getConversationId(),
                messageRequest.getMessages() != null ? messageRequest.getMessages().size() : "null",
                messageRequest.getParticipants() != null ? messageRequest.getParticipants().size() : "null");
        
        // Log the complete message structure for debugging
        if (messageRequest.getMessages() != null && !messageRequest.getMessages().isEmpty()) {
            logger.info("ENRICH-MESSAGES-DEBUG: First 2 messages for messageId={}: [{}]", 
                       messageRequest.getMessageId(),
                       messageRequest.getMessages().stream()
                           .limit(2)
                           .map(msg -> "content='" + (msg.getText() != null ? msg.getText().substring(0, Math.min(50, msg.getContent().length())) : "null") + "'")
                           .collect(java.util.stream.Collectors.joining(", ")));
        }
        
        // Log null/empty checks
        if (messageRequest.getContent() == null) {
            logger.warn("ENRICH-DEBUG: Message content is NULL for messageId={}, taskType={}", 
                       messageRequest.getMessageId(), messageRequest.getTaskType());
        } else if (messageRequest.getContent().trim().isEmpty()) {
            logger.warn("ENRICH-DEBUG: Message content is EMPTY after trim for messageId={}, taskType={}", 
                       messageRequest.getMessageId(), messageRequest.getTaskType());
        }
        
        try {
            // Add detailed logging to identify NPE source
            logger.debug("Message details: messageId={}, taskType={}, preferredProvider={}, hasMessages={}, hasParticipants={}", 
                        messageRequest.getMessageId(), 
                        messageRequest.getTaskType(), 
                        messageRequest.getPreferredProvider(),
                        messageRequest.getMessages() != null ? messageRequest.getMessages().size() : "null",
                        messageRequest.getParticipants() != null ? messageRequest.getParticipants().size() : "null");
            
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
            
            logger.debug("AI Provider obtained: {}", provider.getClass().getSimpleName());
            
            Object result = processEnrichmentTask(provider, messageRequest);
            logger.debug("Enrichment task completed successfully for messageId={}", messageRequest.getMessageId());
            
            // Special logging for conversation enrichment
            if (messageRequest.getTaskType() == AITaskType.ENRICH_CONVERSATION) {
                logger.info("CONVERSATION-ENRICHMENT-RESPONSE: Sending conversation enrichment result to ai-responses topic - messageId={}", 
                           messageRequest.getMessageId());
                System.out.println(">>>>>>> CONSOLE DEBUG: CONVERSATION ENRICHMENT COMPLETE! Sending to ai-responses topic. MessageId: " + 
                                 messageRequest.getMessageId());
            }
            
            // Send response back to reply topic
            sendResponse(messageRequest, result, messageRequest.getTaskType().toString().toLowerCase());
            
            acknowledgment.acknowledge();
            logger.info("Successfully processed enrichment request: messageId={}, taskType={}", 
                       messageRequest.getMessageId(), messageRequest.getTaskType());
            logger.info("=== HANDLE-ENRICHMENT-SUCCESS === MessageId: {}, TaskType: {}", 
                       messageRequest.getMessageId(), messageRequest.getTaskType());
            
            // CONSOLE SUCCESS OUTPUT
            System.out.println(">>>>>>> CONSOLE DEBUG: ENRICHMENT SUCCESS! MessageId: " + 
                             messageRequest.getMessageId() + ", TaskType: " + messageRequest.getTaskType());
            
        } catch (Exception e) {
            logger.error("Failed to process enrichment request: messageId={}, taskType={}, error={}", 
                        messageRequest.getMessageId(), messageRequest.getTaskType(), e.getMessage(), e);
            logger.error("=== HANDLE-ENRICHMENT-ERROR === MessageId: {}, TaskType: {}, Error: {}", 
                        messageRequest.getMessageId(), messageRequest.getTaskType(), e.getMessage());
            
            // CONSOLE ERROR OUTPUT
            System.out.println(">>>>>>> CONSOLE DEBUG: ENRICHMENT ERROR! MessageId: " + 
                             messageRequest.getMessageId() + ", TaskType: " + messageRequest.getTaskType() + 
                             ", Error: " + e.getMessage());
            e.printStackTrace(); // Print full stack trace to console
            
            sendErrorResponse(messageRequest, messageRequest.getTaskType().toString().toLowerCase(), e.getMessage());
            acknowledgment.acknowledge(); // Acknowledge to avoid reprocessing
        }
    }
    
    /**
     * Process different types of enrichment tasks
     */
    private Object processEnrichmentTask(AIProvider provider, AIMessage message) {
        AITaskType taskType = message.getTaskType();
        logger.info("PROCESS-TASK-DEBUG: Processing enrichment task: messageId={}, taskType={}", message.getMessageId(), taskType);
        
        // CONSOLE OUTPUT for task processing
        System.out.println(">>>>>>> CONSOLE DEBUG: processEnrichmentTask called! TaskType: " + taskType + 
                          ", MessageId: " + message.getMessageId());
        
        return switch (taskType) {
            case ENRICH_CONVERSATION -> {
                logger.info("PROCESS-TASK-DEBUG: Processing ENRICH_CONVERSATION for messageId={}", message.getMessageId());
                System.out.println(">>>>>>> CONSOLE DEBUG: Processing ENRICH_CONVERSATION for messageId: " + message.getMessageId());
                if (message.getMessages() == null || message.getMessages().isEmpty()) {
                    throw new IllegalArgumentException("Messages are required for conversation enrichment");
                }
                var participants = convertToSlackParticipants(message);
                logger.info("PROCESS-TASK-DEBUG: Converted {} participants for messageId={}", participants.size(), message.getMessageId());
                yield provider.enrichConversation(message.getMessages(), participants, null);
            }
            case ENRICH_MESSAGE -> {
                logger.info("PROCESS-TASK-DEBUG: Processing ENRICH_MESSAGE for messageId={}", message.getMessageId());
                System.out.println(">>>>>>> CONSOLE DEBUG: Processing ENRICH_MESSAGE for messageId: " + message.getMessageId());
                
                // Add detailed logging for message enrichment
                String content = message.getContent();
                Map<String, Object> context = message.getContext();
                
                System.out.println(">>>>>>> CONSOLE DEBUG: ENRICH_MESSAGE content='" + content + "', context=" + context);
                
                logger.info("ENRICH-MESSAGE-INPUT-DEBUG: About to call provider.enrichMessage() - messageId={}, content='{}', context={}", 
                        message.getMessageId(), content, context);
                
                if (content == null || content.trim().isEmpty()) {
                    logger.warn("ENRICH-MESSAGE-DEBUG: Message content is empty for ENRICH_MESSAGE task, messageId={}, will still call provider", message.getMessageId());
                    // Don't throw an error, let the provider handle it gracefully
                } else {
                    logger.info("ENRICH-MESSAGE-DEBUG: Message content length={} for messageId={}", content.length(), message.getMessageId());
                }
                
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
    
    /**
     * Send successful response back to the reply topic
     * Assumes replyTopic is already set on the AIMessage
     */
    private void sendResponse(AIMessage originalMessage, Object result, String taskType) {
        String replyTopic = originalMessage.getReplyTopic();
        
        // The replyTopic should always be set by now (in handleEnrichmentRequest)
        if (replyTopic == null || replyTopic.trim().isEmpty()) {
            logger.error("CRITICAL: Reply topic is null/empty even after setting default! messageId={}", 
                        originalMessage.getMessageId());
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
            
            // Log whether this was sent to the default ai-responses topic
            if (aiResponsesTopic.equals(replyTopic)) {
                logger.info("SUCCESS: Sent {} response to ai-responses topic: messageId={}, topic={}", 
                           taskType.toUpperCase(), originalMessage.getMessageId(), replyTopic);
                System.out.println(">>>>>>> CONSOLE DEBUG: RESPONSE SENT TO ai.responses.queue! TaskType: " + 
                                 taskType + ", MessageId: " + originalMessage.getMessageId());
            } else {
                logger.info("Sent response to reply topic: messageId={}, replyTopic={}", 
                           originalMessage.getMessageId(), replyTopic);
            }
            
        } catch (Exception e) {
            logger.error("Failed to send response to reply topic: messageId={}, replyTopic={}, error={}", 
                        originalMessage.getMessageId(), replyTopic, e.getMessage(), e);
        }
    }
    
    /**
     * Send error response back to the reply topic
     * Assumes replyTopic is already set on the AIMessage
     */
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