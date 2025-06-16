package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.dto.IngestionMessageEventDTO;
import com.lucid.automation.airouting.model.ConversationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.Collections;
import java.util.List;

/**
 * Manager class for conversation history operations
 * Serves as a facade for the ConversationHistoryService
 */
@Component
public class ConversationHistoryManager {
    
    private static final Logger logger = LoggerFactory.getLogger(ConversationHistoryManager.class);
    
    private final ConversationHistoryService conversationHistoryService;
    
    public ConversationHistoryManager(ConversationHistoryService conversationHistoryService) {
        this.conversationHistoryService = conversationHistoryService;
    }
    
    /**
     * Get all messages for a conversation context
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @param threadTs The thread timestamp (optional)
     * @return List of conversation messages ordered by timestamp
     */
    public List<ConversationMessage> getConversationHistory(String tenantId, String workspaceId, 
                                                         String channelId, String threadTs) {
        if (!validateParams(tenantId, workspaceId, channelId)) {
            return Collections.emptyList();
        }
        
        logger.debug("Getting conversation history: tenantId={}, workspaceId={}, channelId={}, threadTs={}",
                tenantId, workspaceId, channelId, threadTs);
        
        return conversationHistoryService.getMessages(tenantId, workspaceId, channelId, threadTs);
    }
    
    /**
     * Get the most recent N messages from a conversation
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @param threadTs The thread timestamp (optional)
     * @param count The number of most recent messages to retrieve
     * @return List of the most recent conversation messages
     */
    public List<ConversationMessage> getRecentMessages(String tenantId, String workspaceId, 
                                                    String channelId, String threadTs, int count) {
        if (!validateParams(tenantId, workspaceId, channelId) || count <= 0) {
            return Collections.emptyList();
        }
        
        logger.debug("Getting {} recent messages: tenantId={}, workspaceId={}, channelId={}, threadTs={}",
                count, tenantId, workspaceId, channelId, threadTs);
        
        return conversationHistoryService.getRecentMessages(tenantId, workspaceId, channelId, threadTs, count);
    }
    
    /**
     * Delete the oldest N messages from a conversation
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @param threadTs The thread timestamp (optional)
     * @param count The number of oldest messages to delete
     * @return The number of messages deleted
     */
    public int clearOldestMessages(String tenantId, String workspaceId, 
                                String channelId, String threadTs, int count) {
        if (!validateParams(tenantId, workspaceId, channelId) || count <= 0) {
            return 0;
        }
        
        logger.debug("Clearing {} oldest messages: tenantId={}, workspaceId={}, channelId={}, threadTs={}",
                count, tenantId, workspaceId, channelId, threadTs);
        
        return conversationHistoryService.deleteOldestMessages(tenantId, workspaceId, channelId, threadTs, count);
    }
    
    /**
     * Get total message count for a conversation
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @param threadTs The thread timestamp (optional)
     * @return The number of messages in the conversation
     */
    public int getMessageCount(String tenantId, String workspaceId, String channelId, String threadTs) {
        if (!validateParams(tenantId, workspaceId, channelId)) {
            return 0;
        }
        
        List<ConversationMessage> messages = conversationHistoryService.getMessages(
                tenantId, workspaceId, channelId, threadTs);
        
        return messages.size();
    }
    
    /**
     * Check if a conversation has any messages
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @param threadTs The thread timestamp (optional)
     * @return True if the conversation has at least one message, false otherwise
     */
    public boolean hasMessages(String tenantId, String workspaceId, String channelId, String threadTs) {
        return getMessageCount(tenantId, workspaceId, channelId, threadTs) > 0;
    }
    
    /**
     * Validate required parameters
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @return True if all required parameters are valid, false otherwise
     */
    private boolean validateParams(String tenantId, String workspaceId, String channelId) {
        if (!StringUtils.hasText(tenantId)) {
            logger.warn("Invalid tenantId: {}", tenantId);
            return false;
        }
        
        if (!StringUtils.hasText(workspaceId)) {
            logger.warn("Invalid workspaceId: {}", workspaceId);
            return false;
        }
        
        if (!StringUtils.hasText(channelId)) {
            logger.warn("Invalid channelId: {}", channelId);
            return false;
        }
        
        return true;
    }
    
    /**
     * Store a message in the conversation history
     * 
     * @param message The IngestionMessageEventDTO to store
     * @return true if the message was stored successfully, false otherwise
     */
    public boolean storeMessage(IngestionMessageEventDTO message) {
        if (message == null || message.getMessage() == null) {
            logger.warn("Cannot store null message");
            return false;
        }
        
        try {
            ConversationMessage savedMessage = conversationHistoryService.saveMessage(message);
            if (savedMessage != null) {
                logger.debug("Successfully stored message: {}", message.getMessageId());
                
                // Trim conversation if it exceeds maximum size
                String threadTs = message.getThreadTs() != null ? message.getThreadTs() : message.getTimestamp();
                conversationHistoryService.trimConversationIfNeeded(
                        message.getTenantId(), 
                        message.getTeamId(), 
                        message.getChannelId(), 
                        threadTs);
                
                return true;
            }
        } catch (Exception e) {
            logger.error("Error storing message: {}", message.getMessageId(), e);
        }
        
        return false;
    }
}
