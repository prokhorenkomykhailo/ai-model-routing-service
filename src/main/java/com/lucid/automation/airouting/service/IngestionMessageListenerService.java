package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.dto.IngestionEventDTO;
import com.lucid.automation.airouting.model.AIRequest;
import com.lucid.automation.airouting.model.AITaskType;
import com.lucid.automation.airouting.model.Workspace;
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
    private final MessageService messageService;
    private final WorkspaceService workspaceService;
    
    public IngestionMessageListenerService(AIRoutingService aiRoutingService,
                                        MessageService messageService,
                                        WorkspaceService workspaceService) {
        this.aiRoutingService = aiRoutingService;
        this.messageService = messageService;
        this.workspaceService = workspaceService;
    }
    
    /**
     * Processes messages from the ingestion.messages.queue
     * 
     * @param ingestionEvent The ingestion message event received from RabbitMQ
     */
    @RabbitListener(queues = "${rabbitmq.queue.ingestion-messages}")
    public void processIngestionMessage(IngestionEventDTO ingestionEvent) {
        logger.info("Received message from ingestion queue: tenantId={}, messageId={}, channelId={}, userId={}",
                ingestionEvent.getTenantId(), 
                ingestionEvent.getMessage() != null ? ingestionEvent.getMessage().getTs() : null, 
                ingestionEvent.getMessage() != null ? ingestionEvent.getMessage().getChannelId() : null, 
                ingestionEvent.getMessage() != null ? ingestionEvent.getMessage().getUser() : null);
        
        // Log user data information
        if (ingestionEvent.getUser() != null) {
            logger.info("User data: slackUserId={}, name={}, displayName={}", 
                ingestionEvent.getUser().getSlackUserId(),
                ingestionEvent.getUser().getName(),
                ingestionEvent.getUser().getDisplayName());
        } else {
            logger.warn("No user data present in ingestion event");
        }
        
        if (ingestionEvent.getMessage() == null) {
            logger.warn("Received message with null message data, skipping processing");
            return;
        }

        try {
            // Save message to Redis for conversation history
            boolean stored = messageService.storeMessage(ingestionEvent);
            if (stored) {
                logger.info("Successfully saved message to Redis: messageId={}, slackUserId={}", 
                    ingestionEvent.getMessage().getTs(),
                    ingestionEvent.getUser() != null ? ingestionEvent.getUser().getSlackUserId() : "null");
            } else {
                logger.error("Failed to save message to Redis: messageId={}", 
                    ingestionEvent.getMessage().getTs());
            }
            
            // Create or update workspace data
            Workspace workspace = workspaceService.createOrUpdateWorkspace(ingestionEvent);
            if (workspace != null) {
                logger.debug("Updated workspace: {} for tenant: {}", workspace.getId(), workspace.getTenantId());
            }
            
            // Process the message for AI routing
            // processMessage(message);
            
            logger.debug("Successfully processed ingestion message: {}", ingestionEvent.getMessageId());
        } catch (Exception e) {
            logger.error("Error processing ingestion message: {}", ingestionEvent.getMessageId(), e);
            // You might want to implement retry logic or send to a dead letter queue
        }
    }
    
    /**
     * Creates an AIRequest object for the specified task type
     * 
     * @param taskType The type of AI task to perform
     * @param message The ingestion message to process
     * @return The configured AIRequest object
     */
    private AIRequest createAIRequest(AITaskType taskType, IngestionEventDTO message) {
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
    private Map<String, Object> buildContext(IngestionEventDTO message) {
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
            if (message.getMessage().getClientMsgId() != null) {
                context.put("clientMsgId", message.getMessage().getClientMsgId());
            }
            if (message.getMessage().getReplyCount() != null) {
                context.put("replyCount", message.getMessage().getReplyCount());
            }
            if (message.getMessage().getReplyUsers() != null && !message.getMessage().getReplyUsers().isEmpty()) {
                context.put("replyUsers", message.getMessage().getReplyUsers());
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
            if (message.getMetadata().getAdditionalAttributes() != null && !message.getMetadata().getAdditionalAttributes().isEmpty()) {
                context.put("additionalAttributes", message.getMetadata().getAdditionalAttributes());
            }
        }
        
        return context;
    }
}
