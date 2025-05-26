package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.config.RabbitMQConfig;
import com.lucid.automation.airouting.model.AITaskType;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.SlackParticipant;
import com.lucid.automation.airouting.model.message.AIMessage;
import com.lucid.automation.airouting.model.message.SlackMessageData;
import com.lucid.automation.airouting.model.message.SlackParticipantData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Service for publishing AI processing requests to RabbitMQ
 */
@Service
public class AIMessagePublisherService {
    
    private static final Logger logger = LoggerFactory.getLogger(AIMessagePublisherService.class);
    
    private final RabbitTemplate rabbitTemplate;
    private final MessageConverterService messageConverter;
    private final RabbitMQConfig rabbitMQConfig;
    
    @Value("${rabbitmq.exchange.ai-requests:ai.requests}")
    private String aiRequestsExchange;
    
    @Value("${rabbitmq.routing.categorize:ai.categorize}")
    private String categorizeRoutingKey;
    
    @Value("${rabbitmq.routing.summarize:ai.summarize}")
    private String summarizeRoutingKey;
    
    @Value("${rabbitmq.routing.enrich:ai.enrich}")
    private String enrichRoutingKey;
    
    @Autowired
    public AIMessagePublisherService(RabbitTemplate rabbitTemplate,
                                   MessageConverterService messageConverter,
                                   RabbitMQConfig rabbitMQConfig) {
        this.rabbitTemplate = rabbitTemplate;
        this.messageConverter = messageConverter;
        this.rabbitMQConfig = rabbitMQConfig;
    }
    
    /**
     * Publish a categorization request
     */
    public String publishCategorizationRequest(String content, String tenantId, String userId, 
                                             String preferredProvider, String replyTopic) {
        return publishAIRequest(AITaskType.CATEGORIZE, content, tenantId, userId, 
                              null, null, null, null, preferredProvider, replyTopic);
    }
    
    /**
     * Publish a summarization request
     */
    public String publishSummarizationRequest(String content, String tenantId, String userId,
                                            String preferredProvider, String replyTopic) {
        return publishAIRequest(AITaskType.SUMMARIZE, content, tenantId, userId,
                              null, null, null, null, preferredProvider, replyTopic);
    }
    
    /**
     * Publish a conversation enrichment request
     */
    public String publishConversationEnrichmentRequest(String conversationId, 
                                                     List<SlackMessage> messages,
                                                     List<SlackParticipant> participants,
                                                     String tenantId, String userId,
                                                     String preferredProvider, String replyTopic) {
        return publishAIRequest(AITaskType.ENRICH_CONVERSATION, "", tenantId, userId,
                              conversationId, messages, participants, null, 
                              preferredProvider, replyTopic);
    }
    
    /**
     * Publish a message enrichment request
     */
    public String publishMessageEnrichmentRequest(String content, Map<String, Object> context,
                                                String tenantId, String userId,
                                                String preferredProvider, String replyTopic) {
        return publishAIRequest(AITaskType.ENRICH_MESSAGE, content, tenantId, userId,
                              null, null, null, context, preferredProvider, replyTopic);
    }
    
    /**
     * Publish a participant analysis request
     */
    public String publishParticipantAnalysisRequest(SlackParticipant participant,
                                                  List<SlackMessage> messages,
                                                  String tenantId, String userId,
                                                  String preferredProvider, String replyTopic) {
        return publishAIRequest(AITaskType.ANALYZE_PARTICIPANT, "", tenantId, userId,
                              null, messages, List.of(participant), null,
                              preferredProvider, replyTopic);
    }
    
    /**
     * Publish an urgency assessment request
     */
    public String publishUrgencyAssessmentRequest(List<SlackMessage> messages,
                                                String tenantId, String userId,
                                                String preferredProvider, String replyTopic) {
        return publishAIRequest(AITaskType.ASSESS_URGENCY, "", tenantId, userId,
                              null, messages, null, null, preferredProvider, replyTopic);
    }
    
    /**
     * Publish a topic generation request
     */
    public String publishTopicGenerationRequest(List<SlackMessage> messages,
                                              String tenantId, String userId,
                                              String preferredProvider, String replyTopic) {
        return publishAIRequest(AITaskType.GENERATE_TOPIC, "", tenantId, userId,
                              null, messages, null, null, preferredProvider, replyTopic);
    }
    
    /**
     * Publish an entity extraction request
     */
    public String publishEntityExtractionRequest(String content, String tenantId, String userId,
                                               String preferredProvider, String replyTopic) {
        return publishAIRequest(AITaskType.EXTRACT_ENTITIES, content, tenantId, userId,
                              null, null, null, null, preferredProvider, replyTopic);
    }
    
    /**
     * Publish a sentiment analysis request
     */
    public String publishSentimentAnalysisRequest(String content, String tenantId, String userId,
                                                String preferredProvider, String replyTopic) {
        return publishAIRequest(AITaskType.SENTIMENT_ANALYSIS, content, tenantId, userId,
                              null, null, null, null, preferredProvider, replyTopic);
    }
    
    /**
     * Generic method to publish AI requests
     */
    public String publishAIRequest(AITaskType taskType, String content, String tenantId, String userId,
                                 String conversationId, List<SlackMessage> messages, 
                                 List<SlackParticipant> participants, Map<String, Object> context,
                                 String preferredProvider, String replyTopic) {
        
        String messageId = UUID.randomUUID().toString();
        String correlationId = UUID.randomUUID().toString();
        
        try {
            // Create AI message
            AIMessage aiMessage = new AIMessage(messageId, taskType, content != null ? content : "");
            aiMessage.setTenantId(tenantId);
            aiMessage.setUserId(userId);
            aiMessage.setConversationId(conversationId);
            aiMessage.setContext(context);
            aiMessage.setPreferredProvider(preferredProvider);
            aiMessage.setReplyTopic(replyTopic);
            aiMessage.setCorrelationId(correlationId);
            aiMessage.setPriority(determinePriority(taskType));
            
            // Convert messages and participants to lightweight format
            if (messages != null && !messages.isEmpty()) {
                List<SlackMessageData> messageData = messages.stream()
                        .map(messageConverter::convertToMessageData)
                        .collect(Collectors.toList());
                aiMessage.setMessages(messageData);
            }
            
            if (participants != null && !participants.isEmpty()) {
                List<SlackParticipantData> participantData = participants.stream()
                        .map(messageConverter::convertToParticipantData)
                        .collect(Collectors.toList());
                aiMessage.setParticipants(participantData);
            }
            
            // Determine routing key and publish
            String routingKey = getRoutingKeyForTaskType(taskType);
            
            rabbitTemplate.convertAndSend(aiRequestsExchange, routingKey, aiMessage);
            
            logger.info("Published AI request: messageId={}, taskType={}, tenantId={}, routingKey={}", 
                       messageId, taskType, tenantId, routingKey);
            
            return messageId;
            
        } catch (Exception e) {
            logger.error("Failed to publish AI request: taskType={}, tenantId={}, error={}", 
                        taskType, tenantId, e.getMessage(), e);
            throw new RuntimeException("Failed to publish AI request", e);
        }
    }
    
    /**
     * Determine message priority based on task type
     */
    private AIMessage.MessagePriority determinePriority(AITaskType taskType) {
        return switch (taskType) {
            case ASSESS_URGENCY -> AIMessage.MessagePriority.HIGH;
            case CATEGORIZE -> AIMessage.MessagePriority.NORMAL;
            case SENTIMENT_ANALYSIS -> AIMessage.MessagePriority.NORMAL;
            case SUMMARIZE -> AIMessage.MessagePriority.LOW;
            case ENRICH_CONVERSATION, ENRICH_MESSAGE -> AIMessage.MessagePriority.LOW;
            case ANALYZE_PARTICIPANT -> AIMessage.MessagePriority.LOW;
            case GENERATE_TOPIC -> AIMessage.MessagePriority.LOW;
            case EXTRACT_ENTITIES -> AIMessage.MessagePriority.NORMAL;
        };
    }
    
    /**
     * Get routing key based on task type
     */
    private String getRoutingKeyForTaskType(AITaskType taskType) {
        return switch (taskType) {
            case CATEGORIZE -> categorizeRoutingKey;
            case SUMMARIZE -> summarizeRoutingKey;
            case ENRICH_CONVERSATION, ENRICH_MESSAGE, ANALYZE_PARTICIPANT, 
                 ASSESS_URGENCY, GENERATE_TOPIC, EXTRACT_ENTITIES, SENTIMENT_ANALYSIS -> enrichRoutingKey;
        };
    }
}
