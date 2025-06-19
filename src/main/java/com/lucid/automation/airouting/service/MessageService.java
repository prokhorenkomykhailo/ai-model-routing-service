package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.dto.IngestionEventDTO;
import com.lucid.automation.airouting.model.Message;
import com.lucid.automation.airouting.repository.MessageRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Service for managing messages in Redis
 */
@Service
public class MessageService {
    
    private static final Logger logger = LoggerFactory.getLogger(MessageService.class);
    
    private final MessageRepository messageRepository;
    
    @Value("${redis.conversation.max-messages:100}")
    private int maxMessagesPerConversation;
    
    public MessageService(MessageRepository messageRepository) {
        this.messageRepository = messageRepository;
    }
    
    /**
     * Saves a message to Redis
     * 
     * @param ingestionEventDto The ingestion message event to save
     * @return The saved message
     */
    public Message saveMessage(IngestionEventDTO ingestionEventDto) {
        if (ingestionEventDto == null || ingestionEventDto.getMessage() == null) {
            logger.warn("Cannot save null message to Redis");
            return null;
        }
        
        try {
            Message messageToSave = mapToMessage(ingestionEventDto);
            logger.debug("Saving message to Redis: {}", messageToSave.getId());
            logger.debug("Message user data: slackUserId={}, name={}, displayName={}", 
                messageToSave.getSlackUserId(), messageToSave.getName(), messageToSave.getDisplayName());
            
            Message savedMessage = messageRepository.save(messageToSave);
            logger.info("Successfully saved message to Redis with user data: messageId={}, slackUserId={}, name={}", 
                savedMessage.getId(), savedMessage.getSlackUserId(), savedMessage.getName());
            
            return savedMessage;
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
     * @return List of messages sorted by timestamp
     */
    public List<Message> getMessages(String tenantId, String workspaceId, 
                                        String channelId, String threadTs) {
        
        if (!StringUtils.hasText(tenantId) || !StringUtils.hasText(workspaceId) || 
            !StringUtils.hasText(channelId)) {
            logger.warn("Missing required parameters for retrieving messages");
            return Collections.emptyList();
        }
        
        List<Message> messages;
        
        if (StringUtils.hasText(threadTs)) {
            logger.debug("Getting thread messages: tenantId={}, workspaceId={}, channelId={}, threadTs={}",
                    tenantId, workspaceId, channelId, threadTs);
            
            messages = messageRepository.findByTenantIdAndWorkspaceIdAndChannelIdAndThreadTs(
                    tenantId, workspaceId, channelId, threadTs);
        } else {
            logger.debug("Getting channel messages: tenantId={}, workspaceId={}, channelId={}",
                    tenantId, workspaceId, channelId);
            
            messages = messageRepository.findByTenantIdAndWorkspaceIdAndChannelId(
                    tenantId, workspaceId, channelId);
        }
        
        // Sort messages by messageTs (chronological order)
        return messages.stream()
                .sorted(Comparator.comparing(Message::getMessageTs))
                .collect(Collectors.toList());
    }
    
    /**
     * Gets the most recent N messages for a tenant, workspace, channel, and thread
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @param threadTs The thread timestamp (optional)
     * @param count The number of most recent messages to retrieve
     * @return List of the most recent messages sorted by timestamp
     */
    public List<Message> getRecentMessages(String tenantId, String workspaceId, 
                                         String channelId, String threadTs, int count) {
        
        List<Message> allMessages = getMessages(tenantId, workspaceId, channelId, threadTs);
        
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
        
        List<Message> allMessages = getMessages(tenantId, workspaceId, channelId, threadTs);
        
        if (allMessages.isEmpty() || count <= 0) {
            return 0;
        }
        
        // Get the oldest N messages to delete
        int toIndex = Math.min(count, allMessages.size());
        List<Message> messagesToDelete = allMessages.subList(0, toIndex);
        
        int deletedCount = 0;
        for (Message message : messagesToDelete) {
            try {
                messageRepository.deleteById(message.getId());
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
     * Maps an IngestionEventDTO to a Message for Redis storage
     * 
     * @param dto The DTO to map
     * @return The mapped Message
     */
    private Message mapToMessage(IngestionEventDTO dto) {
        logger.debug("Mapping IngestionEventDTO to Message: messageId={}", dto.getMessageId());
        logger.debug("User data present: {}", dto.getUser() != null);
        if (dto.getUser() != null) {
            logger.debug("User details: slackUserId={}, name={}, displayName={}, email={}", 
                dto.getUser().getSlackUserId(), dto.getUser().getName(), 
                dto.getUser().getDisplayName(), dto.getUser().getEmail());
        }
        
        // Extract message timestamp and thread timestamp from the message object
        String messageTs = dto.getMessage() != null ? dto.getMessage().getTs() : null;
        String threadTs = dto.getMessage() != null && dto.getMessage().getThreadTs() != null ? 
            dto.getMessage().getThreadTs() : messageTs;
        
        // Generate a unique ID: tenantId:workspaceId:channelId:threadTs:messageTs
        String id = String.join(":", 
                dto.getTenantId(), 
                dto.getMessage() != null ? dto.getMessage().getTeamId() : "", 
                dto.getMessage() != null ? dto.getMessage().getChannelId() : "", 
                threadTs != null ? threadTs : "", 
                messageTs != null ? messageTs : "");
        
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
        
        Message message = Message.builder()
                .id(id)
                .tenantId(dto.getTenantId())
                .workspaceId(dto.getMessage() != null ? dto.getMessage().getTeamId() : null)
                .channelId(dto.getMessage() != null ? dto.getMessage().getChannelId() : null)
                .threadTs(threadTs)
                .messageTs(messageTs)
                .userId(dto.getMessage() != null ? dto.getMessage().getUser() : null)
                .username(dto.getUser() != null ? dto.getUser().getName() : null)
                .text(dto.getMessage() != null ? dto.getMessage().getText() : null)
                .messageType(dto.getMessage() != null ? dto.getMessage().getType() : null)
                .subtype(dto.getMessage() != null ? dto.getMessage().getSubtype() : null)
                .metadata(metadata)
                .ingestedAt(Instant.now().toEpochMilli())
                // User profile fields from UserData
                .slackUserId(dto.getUser() != null ? dto.getUser().getSlackUserId() : null)
                .teamId(dto.getUser() != null ? dto.getUser().getTeamId() : 
                    (dto.getMessage() != null ? dto.getMessage().getTeamId() : null))
                .name(dto.getUser() != null ? dto.getUser().getName() : null)
                .emailConfirmed(dto.getUser() != null ? dto.getUser().getEmailConfirmed() : null)
                .displayName(dto.getUser() != null ? dto.getUser().getDisplayName() : null)
                .displayNameNormalized(dto.getUser() != null ? dto.getUser().getDisplayNameNormalized() : null)
                .realNameNormalized(dto.getUser() != null ? dto.getUser().getRealNameNormalized() : null)
                .email(dto.getUser() != null ? dto.getUser().getEmail() : null)
                .title(dto.getUser() != null ? dto.getUser().getTitle() : null)
                .phone(dto.getUser() != null ? dto.getUser().getPhone() : null)
                .firstName(dto.getUser() != null ? dto.getUser().getFirstName() : null)
                .lastName(dto.getUser() != null ? dto.getUser().getLastName() : null)
                .pronouns(dto.getUser() != null ? dto.getUser().getPronouns() : null)
                .statusText(dto.getUser() != null ? dto.getUser().getStatusText() : null)
                .avatarHash(dto.getUser() != null ? dto.getUser().getAvatarHash() : null)
                .imageOriginal(dto.getUser() != null ? dto.getUser().getImageOriginal() : null)
                .image24(dto.getUser() != null ? dto.getUser().getImage24() : null)
                .image32(dto.getUser() != null ? dto.getUser().getImage32() : null)
                .image48(dto.getUser() != null ? dto.getUser().getImage48() : null)
                .image72(dto.getUser() != null ? dto.getUser().getImage72() : null)
                .image192(dto.getUser() != null ? dto.getUser().getImage192() : null)
                .image512(dto.getUser() != null ? dto.getUser().getImage512() : null)
                .image1024(dto.getUser() != null ? dto.getUser().getImage1024() : null)
                .teamName(dto.getUser() != null ? dto.getUser().getTeamName() : null)
                .slackUpdatedAt(dto.getUser() != null ? dto.getUser().getSlackUpdatedAt() : null)
                .build();
                
        // Update composite indexes for optimized queries
        message.updateCompositeIndexes();
        
        logger.debug("Mapped message with ID: {}, user: {}, slackUserId: {}", 
            message.getId(), message.getName(), message.getSlackUserId());
        
        return message;
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
        
        List<Message> messages = getMessages(tenantId, workspaceId, channelId, threadTs);
        
        if (messages.size() > maxMessagesPerConversation) {
            int countToDelete = messages.size() - maxMessagesPerConversation;
            deleteOldestMessages(tenantId, workspaceId, channelId, threadTs, countToDelete);
        }
    }

    /**
     * Get all messages for a conversation context (alias for getMessages with validation)
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @param threadTs The thread timestamp (optional)
     * @return List of messages ordered by timestamp
     */
    public List<Message> getConversationHistory(String tenantId, String workspaceId, 
                                             String channelId, String threadTs) {
        if (!validateParams(tenantId, workspaceId, channelId)) {
            return Collections.emptyList();
        }
        
        logger.debug("Getting conversation history: tenantId={}, workspaceId={}, channelId={}, threadTs={}",
                tenantId, workspaceId, channelId, threadTs);
        
        return getMessages(tenantId, workspaceId, channelId, threadTs);
    }

    /**
     * Get the most recent N messages from a conversation with validation
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @param threadTs The thread timestamp (optional)
     * @param count The number of most recent messages to retrieve
     * @return List of the most recent messages
     */
    public List<Message> getRecentMessagesWithValidation(String tenantId, String workspaceId, 
                                        String channelId, String threadTs, int count) {
        if (!validateParams(tenantId, workspaceId, channelId) || count <= 0) {
            return Collections.emptyList();
        }
        
        logger.debug("Getting {} recent messages: tenantId={}, workspaceId={}, channelId={}, threadTs={}",
                count, tenantId, workspaceId, channelId, threadTs);
        
        return getRecentMessages(tenantId, workspaceId, channelId, threadTs, count);
    }

    /**
     * Delete the oldest N messages from a conversation with validation
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
        
        return deleteOldestMessages(tenantId, workspaceId, channelId, threadTs, count);
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
        
        List<Message> messages = getMessages(tenantId, workspaceId, channelId, threadTs);
        
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
     * Store a message in the conversation history with automatic trimming
     * 
     * @param ingestionEventDto The IngestionEventDTO to store
     * @return true if the message was stored successfully, false otherwise
     */
    public boolean storeMessage(IngestionEventDTO ingestionEventDto) {
        if (ingestionEventDto == null || ingestionEventDto.getMessage() == null) {
            logger.warn("Cannot store null message");
            return false;
        }
        
        try {
            Message savedMessage = saveMessage(ingestionEventDto);
            if (savedMessage != null) {
                logger.debug("Successfully stored message: {}", ingestionEventDto.getMessageId());
                // Trim conversation if it exceeds maximum size
                // String threadTs = ingestionEventDto.getThreadTs() != null ? ingestionEventDto.getThreadTs() : ingestionEventDto.getTimestamp();
                // trimConversationIfNeeded(
                //         ingestionEventDto.getTenantId(), 
                //         ingestionEventDto.getTeamId(), 
                //         ingestionEventDto.getChannelId(), 
                //         threadTs);
                
                return true;
            }
        } catch (Exception e) {
            logger.error("Error storing message: {}", ingestionEventDto.getMessageId(), e);
        }
        
        return false;
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
     * Get the first N messages for a workspace ID, ordered by message timestamp
     * 
     * @param workspaceId The workspace ID
     * @param limit The maximum number of messages to retrieve (default 1000 if not specified)
     * @return List of messages ordered by timestamp
     */
    public List<Message> getFirstMessagesForWorkspace(String workspaceId, int limit) {
        if (!StringUtils.hasText(workspaceId) || limit <= 0) {
            logger.warn("Invalid parameters: workspaceId={}, limit={}", workspaceId, limit);
            return Collections.emptyList();
        }
        
        // Create pageable request with sorting by messageTs
        Pageable pageable = PageRequest.of(0, limit, Sort.by(Sort.Direction.ASC, "messageTs"));
        
        logger.debug("Getting first {} messages for workspaceId: {}", limit, workspaceId);
        
        List<Message> messages = messageRepository.findByWorkspaceId(workspaceId, pageable);
        
        logger.info("Retrieved {} messages for workspaceId: {}", messages.size(), workspaceId);
        
        return messages;
    }
    
    /**
     * Get the first 1000 messages for a workspace ID
     * 
     * @param workspaceId The workspace ID
     * @return List of first 1000 messages ordered by timestamp
     */
    public List<Message> getFirst1000MessagesForWorkspace(String workspaceId) {
        return getFirstMessagesForWorkspace(workspaceId, 1000);
    }
    
    /**
     * Get the first N messages for a tenant ID, ordered by message timestamp
     * 
     * @param tenantId The tenant ID
     * @param limit The maximum number of messages to retrieve
     * @return List of messages ordered by timestamp
     */
    public List<Message> getFirstMessagesForTenant(String tenantId, int limit) {
        if (!StringUtils.hasText(tenantId) || limit <= 0) {
            logger.warn("Invalid parameters: tenantId={}, limit={}", tenantId, limit);
            return Collections.emptyList();
        }
        
        // Create pageable request with sorting by messageTs
        Pageable pageable = PageRequest.of(0, limit, Sort.by(Sort.Direction.ASC, "messageTs"));
        
        logger.debug("Getting first {} messages for tenantId: {}", limit, tenantId);
        
        List<Message> messages = messageRepository.findByTenantId(tenantId, pageable);
        
        logger.info("Retrieved {} messages for tenantId: {}", messages.size(), tenantId);
        
        return messages;
    }
    
    /**
     * Get the first N messages for a tenant and workspace, ordered by message timestamp
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @param limit The maximum number of messages to retrieve
     * @return List of messages ordered by timestamp
     */
    public List<Message> getFirstMessagesForTenantAndWorkspace(String tenantId, String workspaceId, int limit) {
        if (!StringUtils.hasText(tenantId) || !StringUtils.hasText(workspaceId) || limit <= 0) {
            logger.warn("Invalid parameters: tenantId={}, workspaceId={}, limit={}", tenantId, workspaceId, limit);
            return Collections.emptyList();
        }
        
        // Create pageable request with sorting by messageTs
        Pageable pageable = PageRequest.of(0, limit, Sort.by(Sort.Direction.ASC, "messageTs"));
        
        logger.debug("Getting first {} messages for tenantId: {}, workspaceId: {}", limit, tenantId, workspaceId);
        
        List<Message> messages = messageRepository.findByTenantIdAndWorkspaceId(tenantId, workspaceId, pageable);
        
        logger.info("Retrieved {} messages for tenantId: {}, workspaceId: {}", messages.size(), tenantId, workspaceId);
        
        return messages;
    }
    
    /**
     * Gets all messages for a workspace and channel
     * 
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @return List of messages sorted by timestamp
     */
    public List<Message> getMessagesByWorkspaceAndChannel(String workspaceId, String channelId) {
        if (!StringUtils.hasText(workspaceId) || !StringUtils.hasText(channelId)) {
            logger.warn("Missing required parameters: workspaceId={}, channelId={}", workspaceId, channelId);
            return Collections.emptyList();
        }
        
        logger.debug("Getting messages for workspaceId: {}, channelId: {}", workspaceId, channelId);
        
        List<Message> messages = messageRepository.findByWorkspaceIdAndChannelId(workspaceId, channelId);
        
        // Sort by messageTs ascending (oldest first)
        messages.sort(Comparator.comparing(Message::getMessageTs, Comparator.nullsLast(Comparator.naturalOrder())));
        
        logger.info("Retrieved {} messages for workspaceId: {}, channelId: {}", messages.size(), workspaceId, channelId);
        
        return messages;
    }
    
    /**
     * Gets all messages for a workspace, channel, and thread
     * 
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @param threadTs The thread timestamp
     * @return List of messages sorted by timestamp
     */
    public List<Message> getMessagesByWorkspaceChannelAndThread(String workspaceId, String channelId, String threadTs) {
        if (!StringUtils.hasText(workspaceId) || !StringUtils.hasText(channelId) || !StringUtils.hasText(threadTs)) {
            logger.warn("Missing required parameters: workspaceId={}, channelId={}, threadTs={}", workspaceId, channelId, threadTs);
            return Collections.emptyList();
        }
        
        logger.debug("Getting messages for workspaceId: {}, channelId: {}, threadTs: {}", workspaceId, channelId, threadTs);
        
        List<Message> messages = messageRepository.findByWorkspaceIdAndChannelIdAndThreadTs(workspaceId, channelId, threadTs);
        
        // Sort by messageTs ascending (oldest first)
        messages.sort(Comparator.comparing(Message::getMessageTs, Comparator.nullsLast(Comparator.naturalOrder())));
        
        logger.info("Retrieved {} messages for workspaceId: {}, channelId: {}, threadTs: {}", messages.size(), workspaceId, channelId, threadTs);
        
        return messages;
    }
    
    /**
     * Gets the first N messages for a workspace and channel
     * 
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @param limit The maximum number of messages to retrieve
     * @return List of messages ordered by timestamp
     */
    public List<Message> getFirstMessagesByWorkspaceAndChannel(String workspaceId, String channelId, int limit) {
        if (!StringUtils.hasText(workspaceId) || !StringUtils.hasText(channelId) || limit <= 0) {
            logger.warn("Invalid parameters: workspaceId={}, channelId={}, limit={}", workspaceId, channelId, limit);
            return Collections.emptyList();
        }
        
        // Create pageable request with sorting by messageTs
        Pageable pageable = PageRequest.of(0, limit, Sort.by(Sort.Direction.ASC, "messageTs"));
        
        logger.debug("Getting first {} messages for workspaceId: {}, channelId: {}", limit, workspaceId, channelId);
        
        Page<Message> messagePage = messageRepository.findByWorkspaceIdAndChannelId(workspaceId, channelId, pageable);
        List<Message> messages = messagePage.getContent();
        
        logger.info("Retrieved {} messages for workspaceId: {}, channelId: {}", messages.size(), workspaceId, channelId);
        
        return messages;
    }
    
    /**
     * Gets the first N messages for a workspace, channel, and thread
     * 
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @param threadTs The thread timestamp
     * @param limit The maximum number of messages to retrieve
     * @return List of messages ordered by timestamp
     */
    public List<Message> getFirstMessagesByWorkspaceChannelAndThread(String workspaceId, String channelId, String threadTs, int limit) {
        if (!StringUtils.hasText(workspaceId) || !StringUtils.hasText(channelId) || !StringUtils.hasText(threadTs) || limit <= 0) {
            logger.warn("Invalid parameters: workspaceId={}, channelId={}, threadTs={}, limit={}", workspaceId, channelId, threadTs, limit);
            return Collections.emptyList();
        }
        
        // Create pageable request with sorting by messageTs
        Pageable pageable = PageRequest.of(0, limit, Sort.by(Sort.Direction.ASC, "messageTs"));
        
        logger.debug("Getting first {} messages for workspaceId: {}, channelId: {}, threadTs: {}", limit, workspaceId, channelId, threadTs);
        
        Page<Message> messagePage = messageRepository.findByWorkspaceIdAndChannelIdAndThreadTs(workspaceId, channelId, threadTs, pageable);
        List<Message> messages = messagePage.getContent();
        
        logger.info("Retrieved {} messages for workspaceId: {}, channelId: {}, threadTs: {}", messages.size(), workspaceId, channelId, threadTs);
        
        return messages;
    }
    
    /**
     * Counts messages for a workspace and channel
     * 
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @return Number of messages
     */
    public long countMessagesByWorkspaceAndChannel(String workspaceId, String channelId) {
        if (!StringUtils.hasText(workspaceId) || !StringUtils.hasText(channelId)) {
            logger.warn("Missing required parameters: workspaceId={}, channelId={}", workspaceId, channelId);
            return 0;
        }
        
        long count = messageRepository.countByWorkspaceIdAndChannelId(workspaceId, channelId);
        logger.debug("Count of messages for workspaceId: {}, channelId: {} is {}", workspaceId, channelId, count);
        
        return count;
    }
    
    /**
     * Counts messages for a workspace, channel, and thread
     * 
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @param threadTs The thread timestamp
     * @return Number of messages
     */
    public long countMessagesByWorkspaceChannelAndThread(String workspaceId, String channelId, String threadTs) {
        if (!StringUtils.hasText(workspaceId) || !StringUtils.hasText(channelId) || !StringUtils.hasText(threadTs)) {
            logger.warn("Missing required parameters: workspaceId={}, channelId={}, threadTs={}", workspaceId, channelId, threadTs);
            return 0;
        }
        
        long count = messageRepository.countByWorkspaceIdAndChannelIdAndThreadTs(workspaceId, channelId, threadTs);
        logger.debug("Count of messages for workspaceId: {}, channelId: {}, threadTs: {} is {}", workspaceId, channelId, threadTs, count);
        
        return count;
    }
    
    // Optimized methods using composite indexes for better performance
    
    /**
     * Get messages by tenant and workspace using optimized composite index
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @return List of messages
     */
    public List<Message> getMessagesByTenantAndWorkspaceOptimized(String tenantId, String workspaceId) {
        if (tenantId == null || workspaceId == null) {
            logger.warn("Missing required parameters: tenantId={}, workspaceId={}", tenantId, workspaceId);
            return new ArrayList<>();
        }
        
        String compositeKey = tenantId + ":" + workspaceId;
        return messageRepository.findByTenantWorkspaceIndex(compositeKey);
    }
    
    /**
     * Get messages by tenant, workspace, and channel using optimized composite index
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @return List of messages
     */
    public List<Message> getMessagesByTenantWorkspaceChannelOptimized(String tenantId, String workspaceId, String channelId) {
        if (tenantId == null || workspaceId == null || channelId == null) {
            logger.warn("Missing required parameters: tenantId={}, workspaceId={}, channelId={}", tenantId, workspaceId, channelId);
            return new ArrayList<>();
        }
        
        String compositeKey = tenantId + ":" + workspaceId + ":" + channelId;
        return messageRepository.findByTenantWorkspaceChannelIndex(compositeKey);
    }
    
    /**
     * Get messages by workspace, channel, and thread using optimized composite index
     * 
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @param threadTs The thread timestamp
     * @return List of messages
     */
    public List<Message> getMessagesByWorkspaceChannelThreadOptimized(String workspaceId, String channelId, String threadTs) {
        if (workspaceId == null || channelId == null || threadTs == null) {
            logger.warn("Missing required parameters: workspaceId={}, channelId={}, threadTs={}", workspaceId, channelId, threadTs);
            return new ArrayList<>();
        }
        
        String compositeKey = workspaceId + ":" + channelId + ":" + threadTs;
        return messageRepository.findByWorkspaceChannelThreadIndex(compositeKey);
    }
    
    /**
     * Get first N messages by tenant and workspace using optimized composite index with pagination
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @param limit Maximum number of messages to return
     * @return Page of messages
     */
    public Page<Message> getFirstMessagesByTenantAndWorkspaceOptimized(String tenantId, String workspaceId, int limit) {
        if (tenantId == null || workspaceId == null) {
            logger.warn("Missing required parameters: tenantId={}, workspaceId={}", tenantId, workspaceId);
            return Page.empty();
        }
        
        String compositeKey = tenantId + ":" + workspaceId;
        Pageable pageable = PageRequest.of(0, limit);
        return messageRepository.findByTenantWorkspaceIndex(compositeKey, pageable);
    }
    
    /**
     * Get first N messages by workspace, channel, and thread using optimized composite index with pagination
     * 
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @param threadTs The thread timestamp
     * @param limit Maximum number of messages to return
     * @return Page of messages
     */
    public Page<Message> getFirstMessagesByWorkspaceChannelThreadOptimized(String workspaceId, String channelId, String threadTs, int limit) {
        if (workspaceId == null || channelId == null || threadTs == null) {
            logger.warn("Missing required parameters: workspaceId={}, channelId={}, threadTs={}", workspaceId, channelId, threadTs);
            return Page.empty();
        }
        
        String compositeKey = workspaceId + ":" + channelId + ":" + threadTs;
        Pageable pageable = PageRequest.of(0, limit);
        return messageRepository.findByWorkspaceChannelThreadIndex(compositeKey, pageable);
    }
    
    /**
     * Count messages by tenant and workspace using optimized composite index
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @return Count of messages
     */
    public long countMessagesByTenantAndWorkspaceOptimized(String tenantId, String workspaceId) {
        if (tenantId == null || workspaceId == null) {
            logger.warn("Missing required parameters: tenantId={}, workspaceId={}", tenantId, workspaceId);
            return 0;
        }
        
        String compositeKey = tenantId + ":" + workspaceId;
        return messageRepository.countByTenantWorkspaceIndex(compositeKey);
    }
    
    /**
     * Count messages by workspace, channel, and thread using optimized composite index
     * 
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @param threadTs The thread timestamp
     * @return Count of messages
     */
    public long countMessagesByWorkspaceChannelThreadOptimized(String workspaceId, String channelId, String threadTs) {
        if (workspaceId == null || channelId == null || threadTs == null) {
            logger.warn("Missing required parameters: workspaceId={}, channelId={}, threadTs={}", workspaceId, channelId, threadTs);
            return 0;
        }
        
        String compositeKey = workspaceId + ":" + channelId + ":" + threadTs;
        return messageRepository.countByWorkspaceChannelThreadIndex(compositeKey);
    }

    // Basic CRUD operations for Message entity
    
    /**
     * Find message by ID
     * 
     * @param id The message ID
     * @return Optional containing the message if found
     */
    public Optional<Message> findById(String id) {
        if (id == null) {
            logger.warn("Cannot find message with null ID");
            return Optional.empty();
        }
        
        try {
            return messageRepository.findById(id);
        } catch (Exception e) {
            logger.error("Error finding message by ID: {}", id, e);
            return Optional.empty();
        }
    }
    
    /**
     * Store/save a message entity
     * 
     * @param message The message entity to store
     * @return The saved message
     */
    public Message storeMessage(Message message) {
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }
        
        // Update composite indexes before saving
        message.updateCompositeIndexes();
        
        try {
            Message savedMessage = messageRepository.save(message);
            logger.debug("Successfully stored message with ID: {}", savedMessage.getId());
            return savedMessage;
        } catch (Exception e) {
            logger.error("Error storing message with ID: {}", message.getId(), e);
            throw e;
        }
    }
    
    /**
     * Delete message by ID
     * 
     * @param id The message ID to delete
     */
    public void deleteById(String id) {
        if (id == null) {
            logger.warn("Cannot delete message with null ID");
            return;
        }
        
        try {
            messageRepository.deleteById(id);
            logger.debug("Successfully deleted message with ID: {}", id);
        } catch (Exception e) {
            logger.error("Error deleting message by ID: {}", id, e);
            throw e;
        }
    }
    
    /**
     * Check if message exists by ID
     * 
     * @param id The message ID
     * @return true if message exists, false otherwise
     */
    public boolean existsById(String id) {
        if (id == null) {
            return false;
        }
        
        try {
            return messageRepository.existsById(id);
        } catch (Exception e) {
            logger.error("Error checking if message exists by ID: {}", id, e);
            return false;
        }
    }
    
    /**
     * Find all messages (with pagination support)
     * Note: For large datasets, consider using more specific query methods
     * 
     * @param pageable Pagination information
     * @return Page of messages
     */
    public Page<Message> findAll(Pageable pageable) {
        try {
            // Since CrudRepository doesn't support pagination directly,
            // we'll need to use a different approach or return a limited set
            logger.warn("findAll with pagination not directly supported by CrudRepository. Consider using specific queries.");
            return Page.empty(pageable);
        } catch (Exception e) {
            logger.error("Error finding all messages", e);
            return Page.empty(pageable);
        }
    }
    
    /**
     * Find messages by tenant workspace index with pagination
     * 
     * @param compositeKey The composite index key
     * @param pageable Pagination information
     * @return Page of messages
     */
    public Page<Message> findByTenantWorkspaceIndex(String compositeKey, Pageable pageable) {
        if (compositeKey == null) {
            logger.warn("Cannot find messages with null composite key");
            return Page.empty(pageable);
        }
        
        try {
            return messageRepository.findByTenantWorkspaceIndex(compositeKey, pageable);
        } catch (Exception e) {
            logger.error("Error finding messages by tenant workspace index: {}", compositeKey, e);
            return Page.empty(pageable);
        }
    }
    
    /**
     * Find messages by workspace channel thread index with pagination
     * 
     * @param compositeKey The composite index key
     * @param pageable Pagination information
     * @return Page of messages
     */
    public Page<Message> findByWorkspaceChannelThreadIndex(String compositeKey, Pageable pageable) {
        if (compositeKey == null) {
            logger.warn("Cannot find messages with null composite key");
            return Page.empty(pageable);
        }
        
        try {
            return messageRepository.findByWorkspaceChannelThreadIndex(compositeKey, pageable);
        } catch (Exception e) {
            logger.error("Error finding messages by workspace channel thread index: {}", compositeKey, e);
            return Page.empty(pageable);
        }
    }
    
    /**
     * Gets all messages with pagination
     * 
     * @param pageable The pagination information
     * @return Page of messages
     */
    public Page<Message> findAllMessages(Pageable pageable) {
        logger.debug("Getting all messages with pagination: page={}, size={}", 
                    pageable.getPageNumber(), pageable.getPageSize());
        
        try {
            // Get all messages from repository
            Iterable<Message> allMessages = messageRepository.findAll();
            List<Message> messageList = new ArrayList<>();
            allMessages.forEach(messageList::add);
            
            // Sort messages by ingestedAt in descending order (most recent first)
            messageList.sort((m1, m2) -> {
                if (m1.getIngestedAt() == null && m2.getIngestedAt() == null) return 0;
                if (m1.getIngestedAt() == null) return 1;
                if (m2.getIngestedAt() == null) return -1;
                return m2.getIngestedAt().compareTo(m1.getIngestedAt());
            });
            
            // Apply pagination manually
            int start = (int) pageable.getOffset();
            int end = Math.min(start + pageable.getPageSize(), messageList.size());
            
            if (start >= messageList.size()) {
                return Page.empty(pageable);
            }
            
            List<Message> pagedMessages = messageList.subList(start, end);
            
            return new org.springframework.data.domain.PageImpl<>(
                pagedMessages, pageable, messageList.size());
                
        } catch (Exception e) {
            logger.error("Error retrieving all messages", e);
            return Page.empty(pageable);
        }
    }

    /**
     * Gets messages by tenant ID with pagination
     * 
     * @param tenantId The tenant ID
     * @param pageable The pagination information
     * @return Page of messages
     */
    public Page<Message> findMessagesByTenantId(String tenantId, Pageable pageable) {
        logger.debug("Getting messages for tenantId: {} with pagination", tenantId);
        
        try {
            List<Message> messages = messageRepository.findByTenantId(tenantId);
            
            // Sort messages by ingestedAt in descending order
            messages.sort((m1, m2) -> {
                if (m1.getIngestedAt() == null && m2.getIngestedAt() == null) return 0;
                if (m1.getIngestedAt() == null) return 1;
                if (m2.getIngestedAt() == null) return -1;
                return m2.getIngestedAt().compareTo(m1.getIngestedAt());
            });
            
            // Apply pagination manually
            int start = (int) pageable.getOffset();
            int end = Math.min(start + pageable.getPageSize(), messages.size());
            
            if (start >= messages.size()) {
                return Page.empty(pageable);
            }
            
            List<Message> pagedMessages = messages.subList(start, end);
            
            return new org.springframework.data.domain.PageImpl<>(
                pagedMessages, pageable, messages.size());
                
        } catch (Exception e) {
            logger.error("Error retrieving messages for tenantId: {}", tenantId, e);
            return Page.empty(pageable);
        }
    }

    /**
     * Gets messages by workspace ID with pagination
     * 
     * @param workspaceId The workspace ID
     * @param pageable The pagination information
     * @return Page of messages
     */
    public Page<Message> findMessagesByWorkspaceId(String workspaceId, Pageable pageable) {
        logger.debug("Getting messages for workspaceId: {} with pagination", workspaceId);
        
        try {
            List<Message> messages = messageRepository.findByWorkspaceId(workspaceId);
            
            // Sort messages by ingestedAt in descending order
            messages.sort((m1, m2) -> {
                if (m1.getIngestedAt() == null && m2.getIngestedAt() == null) return 0;
                if (m1.getIngestedAt() == null) return 1;
                if (m2.getIngestedAt() == null) return -1;
                return m2.getIngestedAt().compareTo(m1.getIngestedAt());
            });
            
            // Apply pagination manually
            int start = (int) pageable.getOffset();
            int end = Math.min(start + pageable.getPageSize(), messages.size());
            
            if (start >= messages.size()) {
                return Page.empty(pageable);
            }
            
            List<Message> pagedMessages = messages.subList(start, end);
            
            return new org.springframework.data.domain.PageImpl<>(
                pagedMessages, pageable, messages.size());
                
        } catch (Exception e) {
            logger.error("Error retrieving messages for workspaceId: {}", workspaceId, e);
            return Page.empty(pageable);
        }
    }

    /**
     * Gets messages by channel ID with pagination
     * 
     * @param channelId The channel ID
     * @param pageable The pagination information
     * @return Page of messages
     */
    public Page<Message> findMessagesByChannelId(String channelId, Pageable pageable) {
        logger.debug("Getting messages for channelId: {} with pagination", channelId);
        
        try {
            List<Message> messages = messageRepository.findByChannelId(channelId);
            
            // Sort messages by ingestedAt in descending order
            messages.sort((m1, m2) -> {
                if (m1.getIngestedAt() == null && m2.getIngestedAt() == null) return 0;
                if (m1.getIngestedAt() == null) return 1;
                if (m2.getIngestedAt() == null) return -1;
                return m2.getIngestedAt().compareTo(m1.getIngestedAt());
            });
            
            // Apply pagination manually
            int start = (int) pageable.getOffset();
            int end = Math.min(start + pageable.getPageSize(), messages.size());
            
            if (start >= messages.size()) {
                return Page.empty(pageable);
            }
            
            List<Message> pagedMessages = messages.subList(start, end);
            
            return new org.springframework.data.domain.PageImpl<>(
                pagedMessages, pageable, messages.size());
                
        } catch (Exception e) {
            logger.error("Error retrieving messages for channelId: {}", channelId, e);
            return Page.empty(pageable);
        }
    }

    /**
     * Gets messages by thread timestamp with pagination
     * 
     * @param threadTs The thread timestamp
     * @param pageable The pagination information
     * @return Page of messages
     */
    public Page<Message> findMessagesByThreadTs(String threadTs, Pageable pageable) {
        logger.debug("Getting messages for threadTs: {} with pagination", threadTs);
        
        try {
            List<Message> messages = messageRepository.findByThreadTs(threadTs);
            
            // Sort messages by ingestedAt in descending order
            messages.sort((m1, m2) -> {
                if (m1.getIngestedAt() == null && m2.getIngestedAt() == null) return 0;
                if (m1.getIngestedAt() == null) return 1;
                if (m2.getIngestedAt() == null) return -1;
                return m2.getIngestedAt().compareTo(m1.getIngestedAt());
            });
            
            // Apply pagination manually
            int start = (int) pageable.getOffset();
            int end = Math.min(start + pageable.getPageSize(), messages.size());
            
            if (start >= messages.size()) {
                return Page.empty(pageable);
            }
            
            List<Message> pagedMessages = messages.subList(start, end);
            
            return new org.springframework.data.domain.PageImpl<>(
                pagedMessages, pageable, messages.size());
                
        } catch (Exception e) {
            logger.error("Error retrieving messages for threadTs: {}", threadTs, e);
            return Page.empty(pageable);
        }
    }
}
