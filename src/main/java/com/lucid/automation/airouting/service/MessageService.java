package com.lucid.automation.airouting.service;

import com.lucid.automation.common.dto.messaging.IngestionEventDTO;
import com.lucid.automation.common.dto.messaging.SlackUserDTO;
import com.lucid.automation.airouting.model.Message;
import com.lucid.automation.airouting.repository.MessageRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Service for managing messages in Redis
 * @author vudu
 */
@Service
public class MessageService {
    
    private static final Logger logger = LoggerFactory.getLogger(MessageService.class);
    
    private final MessageRepository messageRepository;
    private final ChannelService channelService;
    
    @Value("${redis.conversation.max-messages:100}")
    private int maxMessagesPerConversation;
    
    public MessageService(MessageRepository messageRepository, ChannelService channelService) {
        this.messageRepository = messageRepository;
        this.channelService = channelService;
    }
    
    public Message saveMessage(IngestionEventDTO ingestionEventDto) {
        if (isInvalidEventDto(ingestionEventDto)) {
            logger.warn("⚠️ Cannot save null message");
            return null;
        }
        
        try {
            // Create or update channel information first
            channelService.createOrUpdateChannelFromEvent(ingestionEventDto);
            
            Message messageToSave = mapToMessage(ingestionEventDto);
            Message savedMessage = messageRepository.save(messageToSave);
            logger.info("✅ Message saved messageId={}, userId={}", 
                savedMessage.getId(), savedMessage.getSlackUserId());
            return savedMessage;
        } catch (Exception e) {
            logger.error("❌ Failed to save message: {}", e.getMessage());
            return null;
        }
    }
    
    public boolean storeMessage(IngestionEventDTO ingestionEventDto) {
        if (isInvalidEventDto(ingestionEventDto)) {
            logger.warn("⚠️ Cannot store invalid message");
            return false;
        }
        
        try {
            Message savedMessage = saveMessage(ingestionEventDto);
            return savedMessage != null;
        } catch (Exception e) {
            logger.error("❌ Failed to store message {}: {}", 
                ingestionEventDto.getMessage() != null ? ingestionEventDto.getMessage().getTs() : "null", e.getMessage());
            return false;
        }
    }
    
    private boolean isInvalidEventDto(IngestionEventDTO dto) {
        return dto == null || dto.getMessage() == null;
    }
    
    private Message mapToMessage(IngestionEventDTO dto) {
        String messageTs = extractMessageTimestamp(dto);
        String threadTs = extractThreadTimestamp(dto, messageTs);
        String messageId = buildMessageId(dto, threadTs, messageTs);
        
        Message message = createBaseMessage(dto, messageId, messageTs, threadTs);
        setMessageFields(message, dto);
        setUserFields(message, dto);
        message.updateCompositeIndexes();
        
        return message;
    }
    
    private String extractMessageTimestamp(IngestionEventDTO dto) {
        return dto.getMessage() != null ? dto.getMessage().getTs() : null;
    }
    
    private String extractThreadTimestamp(IngestionEventDTO dto, String messageTs) {
        if (dto.getMessage() != null && dto.getMessage().getThreadTs() != null) {
            return dto.getMessage().getThreadTs();
        }
        return messageTs;
    }
    
    private String buildMessageId(IngestionEventDTO dto, String threadTs, String messageTs) {
        return String.join(":", 
            dto.getTenantId(),
            getTeamId(dto),
            getChannelId(dto),
            threadTs != null ? threadTs : "",
            messageTs != null ? messageTs : "");
    }
    
    private String getTeamId(IngestionEventDTO dto) {
        return dto.getMessage() != null ? dto.getMessage().getTeamId() : "";
    }
    
    private String getChannelId(IngestionEventDTO dto) {
        return dto.getMessage() != null ? dto.getMessage().getChannelId() : "";
    }

    public Optional<Message> findById(String id) {
        if (id == null) {
            logger.warn("⚠️ Cannot find message with null ID");
            return Optional.empty();
        }
        
        try {
            return messageRepository.findById(id);
        } catch (Exception e) {
            logger.error("❌ Failed to find message {}: {}", id, e.getMessage());
            return Optional.empty();
        }
    }
    
    public void deleteById(String id) {
        if (id == null) {
            logger.warn("⚠️ Cannot delete message with null ID");
            return;
        }
        
        try {
            messageRepository.deleteById(id);
            logger.info("✅ Message deleted: {}", id);
        } catch (Exception e) {
            logger.error("❌ Failed to delete message {}: {}", id, e.getMessage());
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

    private Message createBaseMessage(IngestionEventDTO dto, String messageId, 
                                    String messageTs, String threadTs) {
        Map<String, Object> metadata = buildMetadata(dto);
        
        return Message.builder()
            .id(messageId)
            .tenantId(dto.getTenantId())
            .tenantSchema(dto.getTenantSchema())
            .deemergeUserId(dto.getDeemergeUserId())
            .messageTs(messageTs)
            .threadTs(threadTs)
            .metadata(metadata)
            .ingestedAt(Instant.now().toEpochMilli())
            .build();
    }
    
    private Map<String, Object> buildMetadata(IngestionEventDTO dto) {
        Map<String, Object> metadata = new HashMap<>();
        if (dto.getMetadata() == null) return metadata;
        
        var meta = dto.getMetadata();
        metadata.put("channelName", meta.getChannelName());
        metadata.put("channelType", meta.getChannelType());
        metadata.put("workspaceName", meta.getWorkspaceName());
        metadata.put("isThreadMessage", meta.isThreadMessage());
        metadata.put("hasAttachments", meta.isHasAttachments());
        metadata.put("mentionedUsers", meta.getMentionedUsers());
        metadata.put("hasReactions", meta.isHasReactions());
        metadata.put("messageLength", meta.getMessageLength());
        metadata.put("containsUrls", meta.isContainsUrls());
        metadata.put("priority", meta.getPriority());
        metadata.put("source", meta.getSource());
        
        return metadata;
    }
    
    private void setMessageFields(Message message, IngestionEventDTO dto) {
        if (dto.getMessage() == null) return;
        
        var msg = dto.getMessage();
        message.setWorkspaceId(msg.getTeamId());
        message.setChannelId(msg.getChannelId());
        message.setChannelName(msg.getChannelName());
        message.setUserId(msg.getUser());
        message.setText(msg.getText());
        message.setMessageType(msg.getType());
        message.setSubtype(msg.getSubtype());
        message.setPermaLink(msg.getPermaLink());
    }
    
    private void setUserFields(Message message, IngestionEventDTO dto) {
        if (dto.getUser() == null) return;
        
        SlackUserDTO user = dto.getUser();
        message.setUsername(user.getName());
        message.setSlackUserId(user.getSlackUserId());
        message.setTeamId(determineTeamId(user, dto));
        message.setName(user.getName());
        message.setEmailConfirmed(user.getEmailConfirmed());
        message.setDisplayName(user.getDisplayName());
        message.setDisplayNameNormalized(user.getDisplayNameNormalized());
        message.setRealNameNormalized(user.getRealNameNormalized());
        message.setEmail(user.getEmail());
        setUserProfileFields(message, user);
        setUserImageFields(message, user);
    }
    
    private String determineTeamId(SlackUserDTO user, IngestionEventDTO dto) {
        if (user.getTeamId() != null) return user.getTeamId();
        if (dto.getMessage() != null) return dto.getMessage().getTeamId();
        return null;
    }
    
    private void setUserProfileFields(Message message, SlackUserDTO user) {
        message.setTitle(user.getTitle());
        message.setPhone(user.getPhone());
        message.setFirstName(user.getFirstName());
        message.setLastName(user.getLastName());
        message.setPronouns(user.getPronouns());
        message.setStatusText(user.getStatusText());
        message.setTeamName(user.getTeamName());
        message.setSlackUpdatedAt(user.getSlackUpdatedAt());
    }
    
    private void setUserImageFields(Message message, SlackUserDTO user) {
        message.setAvatarHash(user.getAvatarHash());
        message.setImageOriginal(user.getImageOriginal());
        message.setImage24(user.getImage24());
        message.setImage32(user.getImage32());
        message.setImage48(user.getImage48());
        message.setImage72(user.getImage72());
        message.setImage192(user.getImage192());
        message.setImage512(user.getImage512());
        message.setImage1024(user.getImage1024());
    }
}
