package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.dto.IngestionMessageEventDTO;
import com.lucid.automation.airouting.model.ConversationMessage;
import com.lucid.automation.airouting.repository.ConversationMessageRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Service for managing conversation history in Redis
 */
@Service
public class ConversationHistoryService {
    
    private static final Logger logger = LoggerFactory.getLogger(ConversationHistoryService.class);
    
    private final ConversationMessageRepository conversationMessageRepository;
    
    @Value("${redis.conversation.max-messages:100}")
    private int maxMessagesPerConversation;
    
    public ConversationHistoryService(ConversationMessageRepository conversationMessageRepository) {
        this.conversationMessageRepository = conversationMessageRepository;
    }
    
    /**
     * Saves a message to Redis
     * 
     * @param message The ingestion message event to save
     * @return The saved conversation message
     */
    public ConversationMessage saveMessage(IngestionMessageEventDTO message) {
        if (message == null || message.getMessage() == null) {
            logger.warn("Cannot save null message to Redis");
            return null;
        }
        
        try {
            ConversationMessage conversationMessage = mapToConversationMessage(message);
            logger.debug("Saving message to Redis: {}", conversationMessage.getId());
            return conversationMessageRepository.save(conversationMessage);
        } catch (Exception e) {
            logger.error("Error saving message to Redis", e);
            return null;
        }
    }
    
    /**
     * Gets all messages for a tenant, workspace, channel, and thread
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @param threadTs The thread timestamp (optional)
     * @return List of conversation messages sorted by timestamp
     */
    public List<ConversationMessage> getMessages(String tenantId, String workspaceId, 
                                              String channelId, String threadTs) {
        
        if (!StringUtils.hasText(tenantId) || !StringUtils.hasText(workspaceId) || !StringUtils.hasText(channelId)) {
            logger.warn("Invalid parameters for retrieving messages: tenantId={}, workspaceId={}, channelId={}",
                    tenantId, workspaceId, channelId);
            return Collections.emptyList();
        }
        
        try {
            List<ConversationMessage> messages;
            
            if (StringUtils.hasText(threadTs)) {
                logger.debug("Retrieving thread messages: tenantId={}, workspaceId={}, channelId={}, threadTs={}",
                        tenantId, workspaceId, channelId, threadTs);
                messages = conversationMessageRepository.findByTenantIdAndWorkspaceIdAndChannelIdAndThreadTs(
                        tenantId, workspaceId, channelId, threadTs);
            } else {
                logger.debug("Retrieving channel messages: tenantId={}, workspaceId={}, channelId={}",
                        tenantId, workspaceId, channelId);
                messages = conversationMessageRepository.findByTenantIdAndWorkspaceIdAndChannelId(
                        tenantId, workspaceId, channelId);
            }
            
            // Sort messages by timestamp
            return messages.stream()
                    .sorted(Comparator.comparing(ConversationMessage::getMessageTs))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            logger.error("Error retrieving messages from Redis", e);
            return Collections.emptyList();
        }
    }
    
    /**
     * Gets the most recent N messages for a tenant, workspace, channel, and thread
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @param threadTs The thread timestamp (optional)
     * @param count The number of most recent messages to retrieve
     * @return List of the most recent conversation messages sorted by timestamp
     */
    public List<ConversationMessage> getRecentMessages(String tenantId, String workspaceId, 
                                                     String channelId, String threadTs, int count) {
        
        List<ConversationMessage> allMessages = getMessages(tenantId, workspaceId, channelId, threadTs);
        
        // Return the most recent N messages
        int fromIndex = Math.max(0, allMessages.size() - count);
        return allMessages.subList(fromIndex, allMessages.size());
    }
    
    /**
     * Deletes the oldest N messages for a tenant, workspace, channel, and thread
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @param threadTs The thread timestamp (optional)
     * @param count The number of oldest messages to delete
     * @return The number of messages deleted
     */
    public int deleteOldestMessages(String tenantId, String workspaceId, 
                                  String channelId, String threadTs, int count) {
        
        List<ConversationMessage> allMessages = getMessages(tenantId, workspaceId, channelId, threadTs);
        
        if (allMessages.isEmpty() || count <= 0) {
            return 0;
        }
        
        // Get the oldest N messages to delete
        int toIndex = Math.min(count, allMessages.size());
        List<ConversationMessage> messagesToDelete = allMessages.subList(0, toIndex);
        
        int deletedCount = 0;
        for (ConversationMessage message : messagesToDelete) {
            try {
                conversationMessageRepository.deleteById(message.getId());
                deletedCount++;
            } catch (Exception e) {
                logger.error("Error deleting message with ID: {}", message.getId(), e);
            }
        }
        
        logger.info("Deleted {} oldest messages for tenantId={}, workspaceId={}, channelId={}, threadTs={}",
                deletedCount, tenantId, workspaceId, channelId, threadTs);
        
        return deletedCount;
    }
    
    /**
     * Maps an IngestionMessageEventDTO to a ConversationMessage for Redis storage
     * 
     * @param dto The DTO to map
     * @return The mapped ConversationMessage
     */
    private ConversationMessage mapToConversationMessage(IngestionMessageEventDTO dto) {
        String messageTs = dto.getTimestamp();
        String threadTs = dto.getThreadTs() != null ? dto.getThreadTs() : messageTs;
        
        // Generate a unique ID: tenantId:workspaceId:channelId:threadTs:messageTs
        String id = String.join(":", 
                dto.getTenantId(), 
                dto.getTeamId(), 
                dto.getChannelId(), 
                threadTs, 
                messageTs);
        
        Map<String, Object> metadata = new HashMap<>();
        if (dto.getMetadata() != null) {
            metadata.put("channelName", dto.getMetadata().getChannelName());
            metadata.put("channelType", dto.getMetadata().getChannelType());
            metadata.put("workspaceName", dto.getMetadata().getWorkspaceName());
            metadata.put("isThreadMessage", dto.getMetadata().isThreadMessage());
            metadata.put("hasAttachments", dto.getMetadata().isHasAttachments());
            metadata.put("mentionedUsers", dto.getMetadata().getMentionedUsers());
            metadata.put("hasReactions", dto.getMetadata().isHasReactions());
            metadata.put("messageLength", dto.getMetadata().getMessageLength());
            metadata.put("containsUrls", dto.getMetadata().isContainsUrls());
            metadata.put("priority", dto.getMetadata().getPriority());
            metadata.put("source", dto.getMetadata().getSource());
        }
        
        return ConversationMessage.builder()
                .id(id)
                .tenantId(dto.getTenantId())
                .workspaceId(dto.getTeamId())
                .channelId(dto.getChannelId())
                .threadTs(threadTs)
                .messageTs(messageTs)
                .userId(dto.getUserId())
                .username(dto.getUsername())
                .text(dto.getText())
                .messageType(dto.getMessageType())
                .subtype(dto.getSubtype())
                .metadata(metadata)
                .ingestedAt(Instant.now().toEpochMilli())
                .build();
    }
    
    /**
     * Ensures the conversation doesn't exceed maximum size by removing oldest messages if needed
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @param threadTs The thread timestamp
     */
    public void trimConversationIfNeeded(String tenantId, String workspaceId, 
                                      String channelId, String threadTs) {
        
        List<ConversationMessage> messages = getMessages(tenantId, workspaceId, channelId, threadTs);
        
        if (messages.size() > maxMessagesPerConversation) {
            int countToDelete = messages.size() - maxMessagesPerConversation;
            deleteOldestMessages(tenantId, workspaceId, channelId, threadTs, countToDelete);
        }
    }
}
