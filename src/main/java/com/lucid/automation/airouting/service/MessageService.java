package com.lucid.automation.airouting.service;

import com.lucid.automation.slackingestion.dto.messaging.IngestionEventDTO;
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
     * Maps an IngestionEventDTO to a Message for Redis storage
     * 
     * @param ingestionEventdto The DTO to map
     * @return The mapped Message
     */
    private Message mapToMessage(IngestionEventDTO ingestionEventdto) {
        logger.debug("User data present: {}", ingestionEventdto.getUser() != null);

        // Extract message timestamp and thread timestamp from the message object
        String messageTs = ingestionEventdto.getMessage() != null ? ingestionEventdto.getMessage().getTs() : null;
        String threadTs = ingestionEventdto.getMessage() != null && ingestionEventdto.getMessage().getThreadTs() != null ?  ingestionEventdto.getMessage().getThreadTs() : messageTs;
        
        // Generate a unique ID: tenantId:workspaceId:channelId:threadTs:messageTs
        String id = String.join(":", 
                ingestionEventdto.getTenantId(), 
                ingestionEventdto.getMessage() != null ? ingestionEventdto.getMessage().getTeamId() : "", 
                ingestionEventdto.getMessage() != null ? ingestionEventdto.getMessage().getChannelId() : "", 
                threadTs != null ? threadTs : "", 
                messageTs != null ? messageTs : "");
        
        Map<String, Object> metadata = new HashMap<>();
        if (ingestionEventdto.getMetadata() != null) {
            metadata.put("channelName", ingestionEventdto.getMetadata().getChannelName());
            metadata.put("channelType", ingestionEventdto.getMetadata().getChannelType());
            metadata.put("workspaceName", ingestionEventdto.getMetadata().getWorkspaceName());
            metadata.put("isThreadMessage", ingestionEventdto.getMetadata().isThreadMessage());
            metadata.put("hasAttachments", ingestionEventdto.getMetadata().isHasAttachments());
            metadata.put("mentionedUsers", ingestionEventdto.getMetadata().getMentionedUsers());
            metadata.put("hasReactions", ingestionEventdto.getMetadata().isHasReactions());
            metadata.put("messageLength", ingestionEventdto.getMetadata().getMessageLength());
            metadata.put("containsUrls", ingestionEventdto.getMetadata().isContainsUrls());
            metadata.put("priority", ingestionEventdto.getMetadata().getPriority());
            metadata.put("source", ingestionEventdto.getMetadata().getSource());
        }

        Message message = Message.builder()
            .id(id)
            .tenantId(ingestionEventdto.getTenantId())
            .tenantSchema(ingestionEventdto.getTenantSchema())
            .deemergeUserId(ingestionEventdto.getDeemergeUserId())
            .messageTs(messageTs)
            .threadTs(threadTs)
            .metadata(metadata)
            .ingestedAt(Instant.now().toEpochMilli())
            .build();
        if (ingestionEventdto.getMessage() != null) {
            message.setWorkspaceId(ingestionEventdto.getMessage().getTeamId());
            message.setChannelId(ingestionEventdto.getMessage().getChannelId());
            message.setChannelName(ingestionEventdto.getMessage().getChannelName());
            message.setUserId(ingestionEventdto.getMessage().getUser());
            message.setText(ingestionEventdto.getMessage().getText());
            message.setMessageType(ingestionEventdto.getMessage().getType());
            message.setSubtype(ingestionEventdto.getMessage().getSubtype());
        }
        if (ingestionEventdto.getUser() != null) {
            message.setUsername(ingestionEventdto.getUser().getName());
            message.setSlackUserId(ingestionEventdto.getUser().getSlackUserId());
            message.setTeamId(ingestionEventdto.getUser().getTeamId() != null ? ingestionEventdto.getUser().getTeamId() : 
                (ingestionEventdto.getMessage() != null ? ingestionEventdto.getMessage().getTeamId() : null));
            message.setName(ingestionEventdto.getUser().getName());
            message.setEmailConfirmed(ingestionEventdto.getUser().getEmailConfirmed());
            message.setDisplayName(ingestionEventdto.getUser().getDisplayName());
            message.setDisplayNameNormalized(ingestionEventdto.getUser().getDisplayNameNormalized());
            message.setRealNameNormalized(ingestionEventdto.getUser().getRealNameNormalized());
            message.setEmail(ingestionEventdto.getUser().getEmail());
            message.setTitle(ingestionEventdto.getUser().getTitle());
            message.setPhone(ingestionEventdto.getUser().getPhone());
            message.setFirstName(ingestionEventdto.getUser().getFirstName());
            message.setLastName(ingestionEventdto.getUser().getLastName());
            message.setPronouns(ingestionEventdto.getUser().getPronouns());
            message.setStatusText(ingestionEventdto.getUser().getStatusText());
            message.setAvatarHash(ingestionEventdto.getUser().getAvatarHash());
            message.setImageOriginal(ingestionEventdto.getUser().getImageOriginal());
            message.setImage24(ingestionEventdto.getUser().getImage24());
            message.setImage32(ingestionEventdto.getUser().getImage32());
            message.setImage48(ingestionEventdto.getUser().getImage48());
            message.setImage72(ingestionEventdto.getUser().getImage72());
            message.setImage192(ingestionEventdto.getUser().getImage192());
            message.setImage512(ingestionEventdto.getUser().getImage512());
            message.setImage1024(ingestionEventdto.getUser().getImage1024());
            message.setTeamName(ingestionEventdto.getUser().getTeamName());
            message.setSlackUpdatedAt(ingestionEventdto.getUser().getSlackUpdatedAt());
        }
                
        // Update composite indexes for optimized queries
        message.updateCompositeIndexes();
        
        logger.debug("Mapped message with ID: {}, user: {}, slackUserId: {}", 
            message.getId(), message.getName(), message.getSlackUserId());
        
        return message;
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

    // Optimized methods using composite indexes for better performance
    
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
            List<Message> messages = messageRepository.findByTenantId(tenantId, pageable);
            
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

}
