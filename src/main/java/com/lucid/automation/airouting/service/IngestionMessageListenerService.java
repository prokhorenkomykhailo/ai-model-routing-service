package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.dto.IngestionMessageEventDTO;
import com.lucid.automation.airouting.model.AIRequest;
import com.lucid.automation.airouting.model.AITaskType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * Service that listens to the ingestion.messages.queue and processes incoming messages
 */
@Service
public class IngestionMessageListenerService {
    
    private static final Logger logger = LoggerFactory.getLogger(IngestionMessageListenerService.class);
    
    private final AIRoutingService aiRoutingService;
    private final ConversationHistoryManager conversationHistoryManager;
    
    public IngestionMessageListenerService(AIRoutingService aiRoutingService,
                                        ConversationHistoryManager conversationHistoryManager) {
        this.aiRoutingService = aiRoutingService;
        this.conversationHistoryManager = conversationHistoryManager;
    }
    
    /**
     * Processes messages from the ingestion.messages.queue
     * 
     * @param message The ingestion message event received from RabbitMQ
     */
    @RabbitListener(queues = "${rabbitmq.queue.ingestion-messages}")
    public void processIngestionMessage(IngestionMessageEventDTO message) {
        logger.info("Received message from ingestion queue: tenantId={}, messageId={}, channelId={}, userId={}",
                message.getTenantId(), message.getMessageId(), message.getChannelId(), message.getUserId());
        
        if (message.getMessage() == null) {
            logger.warn("Received message with null message data, skipping processing");
            return;
        }
        
        if (logger.isDebugEnabled()) {
            logger.debug("Message details: type={}, subtype={}, teamId={}, conversationGroupId={}, ts={}, ingestedAt={}",
                message.getMessageType(),
                message.getSubtype(),
                message.getTeamId(),
                message.getConversationGroupId(),
                message.getTimestamp(),
                message.getMessage().getIngestedAt());
        }
        
        try {
            // Save message to Redis for conversation history
            boolean stored = conversationHistoryManager.storeMessage(message);
            if (stored) {
                logger.debug("Saved message to Redis: {}", message.getMessageId());
            }
            
            // Process the message
            processMessage(message);
            
            logger.debug("Successfully processed ingestion message: {}", message.getMessageId());
        } catch (Exception e) {
            logger.error("Error processing ingestion message: {}", message.getMessageId(), e);
            // You might want to implement retry logic or send to a dead letter queue
        }
    }
    
    /**
     * Processes the message by routing it to appropriate AI services
     * 
     * @param message The ingestion message to process
     */
    private void processMessage(IngestionMessageEventDTO message) {
        // Step 1: Check if message meets criteria for processing
        if (message.getText() == null || message.getText().isEmpty()) {
            logger.debug("Skipping empty message: {}", message.getMessageId());
            return;
        }
        
        // Skip system messages like channel_join, channel_leave, etc.
        if (message.getSubtype() != null && 
            (message.getSubtype().equals("channel_join") || 
             message.getSubtype().equals("channel_leave") ||
             message.getSubtype().equals("bot_message"))) {
            logger.debug("Skipping system message with subtype: {}", message.getSubtype());
            return;
        }
        
        logger.info("Processing message: id={}, text='{}', type={}, subtype={}", 
                message.getMessageId(), 
                message.getText().length() > 50 ? message.getText().substring(0, 47) + "..." : message.getText(),
                message.getMessageType(),
                message.getSubtype());
        
        // Step 2: Route to appropriate AI tasks based on content
        try {
            // Categorize the message
            AIRequest categorizeRequest = createAIRequest(AITaskType.CATEGORIZE, message);
            aiRoutingService.processRequest(categorizeRequest);
            
            // Analyze sentiment
            AIRequest sentimentRequest = createAIRequest(AITaskType.SENTIMENT_ANALYSIS, message);
            aiRoutingService.processRequest(sentimentRequest);
            
            // Generate topic
            AIRequest topicRequest = createAIRequest(AITaskType.GENERATE_TOPIC, message);
            aiRoutingService.processRequest(topicRequest);
            
            // Extract entities
            AIRequest entityRequest = createAIRequest(AITaskType.EXTRACT_ENTITIES, message);
            aiRoutingService.processRequest(entityRequest);
            
            logger.info("Message processing routed successfully: {}", message.getMessageId());
        } catch (Exception e) {
            logger.error("Error routing AI tasks for message: {}", message.getMessageId(), e);
        }
    }
    
    /**
     * Creates an AIRequest object for the specified task type
     * 
     * @param taskType The type of AI task to perform
     * @param message The ingestion message to process
     * @return The configured AIRequest object
     */
    private AIRequest createAIRequest(AITaskType taskType, IngestionMessageEventDTO message) {
        AIRequest request = new AIRequest();
        request.setTaskType(taskType);
        request.setContent(message.getText());
        request.setTenantId(message.getTenantId());
        
        // Set conversation ID if available
        if (message.getConversationGroupId() != null) {
            request.setConversationId(message.getConversationGroupId());
        } else {
            // Use channel ID as fallback for conversation ID
            request.setConversationId(message.getChannelId());
        }
        
        // Set user ID if available
        if (message.getUserId() != null) {
            request.setUserId(message.getUserId());
        }
        
        // Add context from message metadata
        Map<String, Object> context = buildContext(message);
        request.setContext(context);
        
        return request;
    }
    
    /**
     * Builds context map for AI tasks from the message
     * 
     * @param message The ingestion message
     * @return Map of context values for AI processing
     */
    private Map<String, Object> buildContext(IngestionMessageEventDTO message) {
        Map<String, Object> context = new HashMap<>();
        context.put("messageId", message.getMessageId());
        context.put("channelId", message.getChannelId());
        context.put("teamId", message.getTeamId());
        context.put("userId", message.getUserId());
        context.put("username", message.getUsername());
        context.put("timestamp", message.getTimestamp());
        context.put("threadTs", message.getThreadTs());
        context.put("conversationGroupId", message.getConversationGroupId());
        context.put("messageType", message.getMessageType());
        context.put("subtype", message.getSubtype());
        context.put("tenantId", message.getTenantId());
        context.put("tenantSchema", message.getTenantSchema());
        
        // Add additional fields from the message object if available
        if (message.getMessage() != null) {
            if (message.getMessage().getTopic() != null) {
                context.put("topic", message.getMessage().getTopic());
            }
            if (message.getMessage().getPurpose() != null) {
                context.put("purpose", message.getMessage().getPurpose());
            }
            if (message.getMessage().getReplyCount() != null) {
                context.put("replyCount", message.getMessage().getReplyCount());
            }
            if (message.getMessage().getReplyUsersCount() != null) {
                context.put("replyUsersCount", message.getMessage().getReplyUsersCount());
            }
            if (message.getMessage().getLatestReply() != null) {
                context.put("latestReply", message.getMessage().getLatestReply());
            }
        }
        
        if (message.getMetadata() != null) {
            context.put("channelName", message.getMetadata().getChannelName());
            context.put("channelType", message.getMetadata().getChannelType());
            context.put("workspaceName", message.getMetadata().getWorkspaceName());
            context.put("isThreadMessage", message.getMetadata().isThreadMessage());
            context.put("messageLength", message.getMetadata().getMessageLength());
            context.put("hasAttachments", message.getMetadata().isHasAttachments());
            context.put("containsUrls", message.getMetadata().isContainsUrls());
            context.put("hasReactions", message.getMetadata().isHasReactions());
            
            if (message.getMetadata().getPriority() != null) {
                context.put("priority", message.getMetadata().getPriority());
            }
            if (message.getMetadata().getSource() != null) {
                context.put("source", message.getMetadata().getSource());
            }
            if (message.getMetadata().getMentionedUsers() != null && !message.getMetadata().getMentionedUsers().isEmpty()) {
                context.put("mentionedUsers", message.getMetadata().getMentionedUsers());
            }
        }
        
        return context;
    }
}
