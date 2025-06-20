package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.model.request.ConversationEnrichmentRequest;
import com.lucid.automation.airouting.model.AITaskType;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.SlackParticipant;
import com.lucid.automation.airouting.model.message.AIMessage;
import com.lucid.automation.airouting.model.message.SlackParticipantData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Service for publishing AI processing requests to Kafka
 */
@Service
public class AIMessagePublisherService {
    
    private static final Logger logger = LoggerFactory.getLogger(AIMessagePublisherService.class);
    
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final MessageConverterService messageConverter;
    
    @Value("${kafka.topics.ai-categorize:ai-categorize}")
    private String categorizeTopic;
    
    @Value("${kafka.topics.ai-summarize:ai-summarize}")
    private String summarizeTopic;
    
    @Value("${kafka.topics.ai-enrich:ai-enrich}")
    private String enrichTopic;
    
    public AIMessagePublisherService(KafkaTemplate<String, Object> kafkaTemplate,
                                   MessageConverterService messageConverter) {
        this.kafkaTemplate = kafkaTemplate;
        this.messageConverter = messageConverter;
    }
    
    /**
     * Publish a categorization request
     */
    public String publishCategorizationRequest(String content, String tenantId, String tenantSchema,
                                             String userId, String preferredProvider, String replyTopic) {
        return publishAIRequest(AITaskType.CATEGORIZE, content, tenantId, tenantSchema, userId, 
                              null, null, null, null, preferredProvider, replyTopic);
    }
    
    /**
     * Publish a summarization request
     */
    public String publishSummarizationRequest(String content, String tenantId, String tenantSchema,
                                            String userId, String preferredProvider, String replyTopic) {
        return publishAIRequest(AITaskType.SUMMARIZE, content, tenantId, tenantSchema, userId,
                              null, null, null, null, preferredProvider, replyTopic);
    }
    
    /**
     * Publish a conversation enrichment request using a request object
     */
    public String publishConversationEnrichmentRequest(ConversationEnrichmentRequest request, String userId) {
        return publishAIRequest(AITaskType.ENRICH_CONVERSATION, "", request.getTenantId(), request.getTenantSchema(),
                              userId, request.getConversationId(), request.getMessages(), request.getParticipants(), 
                              request.getContext(), request.getPreferredProvider(), request.getReplyTopic());
    }
    
    
    /**
     * Publish a message enrichment request
     */
    public String publishMessageEnrichmentRequest(String content, Map<String, Object> context,
                                                String tenantId, String tenantSchema, String userId,
                                                String preferredProvider, String replyTopic) {
        return publishAIRequest(AITaskType.ENRICH_MESSAGE, content, tenantId, tenantSchema, userId,
                              null, null, null, context, preferredProvider, replyTopic);
    }
    
    /**
     * Generic method to publish AI requests
     */
    public String publishAIRequest(AITaskType taskType, String content, String tenantId, String tenantSchema,
                                 String userId, String conversationId, List<SlackMessage> messages, 
                                 List<SlackParticipant> participants, Map<String, Object> context,
                                 String preferredProvider, String replyTopic) {
        
        String messageId = UUID.randomUUID().toString();
        String correlationId = UUID.randomUUID().toString();
        
        try {
            // Create AI message
            AIMessage aiMessage = new AIMessage(messageId, taskType, content != null ? content : "");
            aiMessage.setTenantId(tenantId);
            aiMessage.setTenantSchema(tenantSchema);
            aiMessage.setUserId(userId);
            aiMessage.setConversationId(conversationId);
            aiMessage.setContext(context);
            aiMessage.setPreferredProvider(preferredProvider);
            aiMessage.setReplyTopic(replyTopic);
            aiMessage.setCorrelationId(correlationId);
            aiMessage.setPriority(determinePriority(taskType));
            
            // Set messages and participants directly (no conversion needed)
            if (messages != null && !messages.isEmpty()) {
                aiMessage.setMessages(messages);
            }
            
            if (participants != null && !participants.isEmpty()) {
                List<SlackParticipantData> participantData = participants.stream()
                        .map(messageConverter::convertToParticipantData)
                        .collect(Collectors.toList());
                aiMessage.setParticipants(participantData);
            }
            
            // Determine topic and publish
            String topic = getTopicForTaskType(taskType);
            
            kafkaTemplate.send(topic, aiMessage);
            
            logger.info("Published AI request: messageId={}, taskType={}, tenantId={}, topic={}", 
                       messageId, taskType, tenantId, topic);
            
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
     * Get Kafka topic based on task type
     */
    private String getTopicForTaskType(AITaskType taskType) {
        return switch (taskType) {
            case CATEGORIZE -> categorizeTopic;
            case SUMMARIZE -> summarizeTopic;
            case ENRICH_CONVERSATION, ENRICH_MESSAGE, ANALYZE_PARTICIPANT, 
                 ASSESS_URGENCY, GENERATE_TOPIC, EXTRACT_ENTITIES, SENTIMENT_ANALYSIS -> enrichTopic;
        };
    }
}
