package com.lucid.automation.airouting.service;

import com.lucid.automation.common.dto.messaging.IngestionEventDTO;
import com.lucid.automation.common.dto.messaging.IngestionUserDTO;
import com.lucid.automation.common.dto.messaging.IngestionMessageDTO;
import com.lucid.automation.common.dto.messaging.MetadataDTO;
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
        String tenantId = dto.getTenantId() != null ? dto.getTenantId() : "";
        String workspaceId = dto.getMessage() != null ? dto.getMessage().getBestWorkspaceId() : "";
        String channelId = getChannelId(dto);
        return String.join(":",
            tenantId,
            workspaceId != null ? workspaceId : "",
            channelId,
            threadTs != null ? threadTs : "",
            messageTs != null ? messageTs : "");
    }

    private String getTeamId(IngestionEventDTO dto) {
        // Legacy name: this is effectively the best available workspace identifier.
        return dto.getMessage() != null ? dto.getMessage().getBestWorkspaceId() : "";
    }

    private String getChannelId(IngestionEventDTO dto) {
        return dto.getMessage() != null && dto.getMessage().getChannelId() != null ? dto.getMessage().getChannelId() : "";
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

    // ==================== Soft Delete Methods ====================

    /**
     * Soft delete a message by marking it as deleted without removing from Redis
     * The message will be excluded from default queries but retained for audit/recovery
     *
     * @param id The message ID
     * @param deletedBy User or admin ID who triggered the deletion
     * @param reason Deletion reason (e.g., MANUAL, CLEANUP, EXPIRED, ADMIN)
     * @author vudu
     */
    public void softDeleteById(String id, String deletedBy, String reason) {
        if (id == null) {
            logger.warn("⚠️ [SOFT-DELETE] Cannot delete message with null ID");
            return;
        }

        try {
            Optional<Message> messageOpt = messageRepository.findById(id);
            if (messageOpt.isEmpty()) {
                logger.warn("⚠️ [SOFT-DELETE] Message not found: {}", id);
                return;
            }

            Message message = messageOpt.get();

            // Mark as deleted
            message.setIsDeleted(true);
            message.setDeletedAt(System.currentTimeMillis());
            message.setDeletionReason(reason != null ? reason : "MANUAL");
            message.setDeletedBy(deletedBy != null ? deletedBy : "SYSTEM");

            // Calculate retention expiry (30 days from deletion)
            long retentionDays = 30;
            long retentionExpiryTimestamp = message.getDeletedAt() + (retentionDays * 24 * 60 * 60 * 1000L);
            message.setRetentionExpiry(retentionExpiryTimestamp);

            messageRepository.save(message);

            logger.info("✅ [SOFT-DELETE] Message marked deleted | ID: {} | By: {} | Reason: {} | ExpiresAt: {}",
                    id, message.getDeletedBy(), message.getDeletionReason(),
                    Instant.ofEpochMilli(message.getRetentionExpiry()));
        } catch (Exception e) {
            logger.error("❌ [SOFT-DELETE] Failed to soft delete message {}: {}", id, e.getMessage());
            throw e;
        }
    }

    /**
     * Batch soft delete messages by marking them as deleted without removing from Redis.
     * Optimized version that uses batch operations to prevent N+1 query problem.
     * Messages are excluded from default queries but retained for audit/recovery (30 days).
     *
     * @param messageIds List of message IDs to soft delete
     * @param deletedBy User or admin ID who triggered the deletion
     * @param reason Deletion reason (e.g., AI_PROCESSING_COMPLETE, BATCH_CLEANUP)
     * @return Number of messages successfully soft deleted
     * @author vudu
     */
    public int softDeleteByIds(List<String> messageIds, String deletedBy, String reason) {
        if (messageIds == null || messageIds.isEmpty()) {
            logger.debug("⚠️ [BATCH-SOFT-DELETE] No message IDs provided");
            return 0;
        }

        long startTime = System.currentTimeMillis();
        logger.info("🧹 [BATCH-SOFT-DELETE] Starting batch deletion of {} messages | By: {} | Reason: {}",
                   messageIds.size(), deletedBy, reason);

        try {
            // Batch fetch all messages (1 operation instead of N)
            List<Message> messages = new ArrayList<>();
            messageRepository.findAllById(messageIds).forEach(messages::add);

            if (messages.isEmpty()) {
                logger.warn("⚠️ [BATCH-SOFT-DELETE] No messages found for provided IDs");
                return 0;
            }

            long currentTime = System.currentTimeMillis();
            long retentionDays = 30;
            long retentionExpiryTimestamp = currentTime + (retentionDays * 24 * 60 * 60 * 1000L);
            String effectiveDeletedBy = deletedBy != null ? deletedBy : "SYSTEM";
            String effectiveReason = reason != null ? reason : "MANUAL";

            // Batch update all messages in memory
            int updatedCount = 0;
            for (Message message : messages) {
                // Skip already deleted messages
                if (Boolean.TRUE.equals(message.getIsDeleted())) {
                    continue;
                }

                message.setIsDeleted(true);
                message.setDeletedAt(currentTime);
                message.setDeletionReason(effectiveReason);
                message.setDeletedBy(effectiveDeletedBy);
                message.setRetentionExpiry(retentionExpiryTimestamp);
                updatedCount++;
            }

            // Batch save all messages (1 operation instead of N)
            messageRepository.saveAll(messages);

            long duration = System.currentTimeMillis() - startTime;
            logger.info("✅ [BATCH-SOFT-DELETE] Successfully marked {} messages as deleted in {}ms | " +
                       "By: {} | Reason: {} | ExpiresAt: {} | Performance: {}/sec",
                       updatedCount, duration, effectiveDeletedBy, effectiveReason,
                       Instant.ofEpochMilli(retentionExpiryTimestamp),
                       updatedCount > 0 ? String.format("%.1f", (updatedCount * 1000.0) / duration) : "N/A");

            return updatedCount;

        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            logger.error("❌ [BATCH-SOFT-DELETE] Failed after {}ms: {}", duration, e.getMessage(), e);
            throw new RuntimeException("Batch soft delete failed", e);
        }
    }

    /**
     * Hard delete an expired message (physical removal from Redis)
     * This method should ONLY be called by the retention cleanup job
     *
     * @param id The message ID
     * @author vudu
     */
    public void hardDeleteExpired(String id) {
        if (id == null) {
            logger.warn("⚠️ [HARD-DELETE] Cannot delete message with null ID");
            return;
        }

        try {
            messageRepository.deleteById(id);
            logger.info("🗑️ [HARD-DELETE] Expired message physically removed from Redis: {}", id);
        } catch (Exception e) {
            logger.error("❌ [HARD-DELETE] Failed to hard delete expired message {}: {}", id, e.getMessage());
            throw e;
        }
    }

    /**
     * Restore a soft-deleted message (undelete)
     * Used by admin endpoints to recover accidentally deleted messages
     *
     * @param id The message ID
     * @author vudu
     */
    public void restoreDeletedMessage(String id) {
        if (id == null) {
            logger.warn("⚠️ [RESTORE] Cannot restore message with null ID");
            return;
        }

        try {
            Optional<Message> messageOpt = messageRepository.findById(id);
            if (messageOpt.isEmpty()) {
                logger.warn("⚠️ [RESTORE] Message not found: {}", id);
                return;
            }

            Message message = messageOpt.get();

            // Clear deletion fields
            message.setIsDeleted(false);
            message.setDeletedAt(null);
            message.setDeletionReason(null);
            message.setDeletedBy(null);
            message.setRetentionExpiry(null);

            messageRepository.save(message);

            logger.info("✅ [RESTORE] Message restored: {}", id);
        } catch (Exception e) {
            logger.error("❌ [RESTORE] Failed to restore message {}: {}", id, e.getMessage());
            throw e;
        }
    }

    // ==================== End Soft Delete Methods ====================

    /**
     * Deletes all messages from the repository
     */
    public void deleteAllMessages() {
        try {
            logger.warn("🗑️ Deleting ALL messages from repository");
            messageRepository.deleteAll();
            logger.info("✅ All messages deleted successfully");
        } catch (Exception e) {
            logger.error("❌ Failed to delete all messages: {}", e.getMessage());
            throw e;
        }
    }



    /**
     * Gets all messages with pagination (excludes soft-deleted messages by default).
     *
     * @param pageable The pagination information
     * @return Page of active (non-deleted) messages
     * @author vudu
     */
    public Page<Message> findAllMessages(Pageable pageable) {
        logger.debug("Getting all active messages with pagination: page={}, size={}",
                    pageable.getPageNumber(), pageable.getPageSize());

        try {
            // Get all messages from repository
            Iterable<Message> allMessages = messageRepository.findAll();
            List<Message> messageList = new ArrayList<>();
            allMessages.forEach(messageList::add);

            // Filter out soft-deleted messages
            messageList = messageList.stream()
                    .filter(m -> !Boolean.TRUE.equals(m.getIsDeleted()))
                    .collect(java.util.stream.Collectors.toList());

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
            logger.error("Error retrieving all active messages", e);
            return Page.empty(pageable);
        }
    }

    /**
     * Gets messages by tenant ID with pagination (excludes soft-deleted messages by default).
     *
     * @param tenantId The tenant ID
     * @param pageable The pagination information
     * @return Page of active (non-deleted) messages for the tenant
     * @author vudu
     */
    public Page<Message> findMessagesByTenantId(String tenantId, Pageable pageable) {
        logger.debug("Getting active messages for tenantId: {} with pagination", tenantId);

        try {
            // Get ALL active messages first (no pagination yet) to calculate total count
            List<Message> allActiveMessages = messageRepository.findAllByTenantId(tenantId).stream()
                    .filter(msg -> msg.getIsDeleted() == null || !msg.getIsDeleted())
                    .toList();

            // Sort by ingestedAt descending
            List<Message> sortedMessages = allActiveMessages.stream()
                    .sorted((m1, m2) -> {
                        if (m1.getIngestedAt() == null && m2.getIngestedAt() == null) return 0;
                        if (m1.getIngestedAt() == null) return 1;
                        if (m2.getIngestedAt() == null) return -1;
                        return m2.getIngestedAt().compareTo(m1.getIngestedAt());
                    })
                    .toList();

            // Apply pagination manually
            int start = (int) pageable.getOffset();
            int end = Math.min(start + pageable.getPageSize(), sortedMessages.size());

            if (start >= sortedMessages.size()) {
                return Page.empty(pageable);
            }

            List<Message> pagedMessages = sortedMessages.subList(start, end);

            return new org.springframework.data.domain.PageImpl<>(
                pagedMessages, pageable, sortedMessages.size());

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
            .isProcessed(false)  // Initialize as unprocessed
            .build();
    }

    private Map<String, Object> buildMetadata(IngestionEventDTO dto) {
        Map<String, Object> metadata = new HashMap<>();
        if (dto.getMetadata() == null) return metadata;

        // Use unified MetadataDTO for all platforms (Slack, Gmail)
        MetadataDTO meta = dto.getMetadata();

        // Common fields (all platforms)
        metadata.put("channelName", meta.getChannelName());
        metadata.put("channelType", meta.getChannelType());
        metadata.put("workspaceName", meta.getWorkspaceName());
        metadata.put("isThreadMessage", meta.getIsThreadMessage());
        metadata.put("hasAttachments", meta.getHasAttachments());
        metadata.put("mentionedUsers", meta.getMentionedUsers());
        metadata.put("hasReactions", meta.getHasReactions());
        metadata.put("messageLength", meta.getMessageLength());
        metadata.put("containsUrls", meta.getContainsUrls());
        metadata.put("priority", meta.getPriority());
        metadata.put("source", meta.getSource());
        metadata.put("oldestTs", meta.getOldestTs());
        metadata.put("additionalAttributes", meta.getAdditionalAttributes());

        // Gmail-specific fields (only present for Gmail messages)
        if (dto.isGmailMessage()) {
            metadata.put("to", meta.getTo());
            metadata.put("cc", meta.getCc());
            metadata.put("bcc", meta.getBcc());
            metadata.put("from", meta.getFrom());
            metadata.put("subject", meta.getSubject());
            metadata.put("gmailMessageId", meta.getGmailMessageId());
            metadata.put("threadId", meta.getThreadId());
            metadata.put("historyId", meta.getHistoryId());
            metadata.put("internalDate", meta.getInternalDate());
            metadata.put("rfcMessageId", meta.getRfcMessageId());
        }

        return metadata;
    }

    private void setMessageFields(Message message, IngestionEventDTO dto) {
        if (dto.getMessage() == null) return;

        IngestionMessageDTO msg = dto.getMessage();
        // Use unified message DTO - workspaceId is now directly available
        message.setWorkspaceId(msg.getBestWorkspaceId());
        message.setChannelId(msg.getChannelId());
        message.setChannelName(msg.getChannelName());
        message.setUserId(msg.getUser());
        message.setText(msg.getText());
        message.setMessageType(msg.getType());
        message.setSubtype(msg.getSubtype());
        message.setSource(msg.getSource());
        message.setPermaLink(msg.getPermaLink());
    }

    private void setUserFields(Message message, IngestionEventDTO dto) {
        if (dto.getUser() == null) return;

        IngestionUserDTO user = dto.getUser();
        message.setUsername(user.getName());

        // Set both uniqueUserId (preferred) and slackUserId (legacy fallback)
        message.setUniqueUserId(user.getUniqueUserId());
        message.setSlackUserId(user.getSlackUserId());

        message.setTeamId(determineTeamId(user, dto));
        message.setName(user.getName());
        message.setEmailConfirmed(user.getEmailVerified());
        message.setDisplayName(user.getDisplayName());
        message.setDisplayNameNormalized(user.getDisplayNameNormalized());
        message.setRealNameNormalized(user.getRealNameNormalized());
        message.setEmail(user.getEmail());
        setUserProfileFields(message, user);
        setUserImageFields(message, user);
    }

    private String determineTeamId(IngestionUserDTO user, IngestionEventDTO dto) {
        if (user.getTeamId() != null) return user.getTeamId();
        IngestionMessageDTO message = dto.getMessage();
        if (message != null) return message.getBestWorkspaceId();
        return null;
    }

    private void setUserProfileFields(Message message, IngestionUserDTO user) {
        message.setTitle(user.getTitle());
        message.setPhone(user.getPhone());
        message.setFirstName(user.getFirstName());
        message.setLastName(user.getLastName());
        message.setPronouns(user.getPronouns());
        message.setStatusText(user.getStatusText());
        message.setTeamName(user.getTeamName());
        message.setSlackUpdatedAt(user.getSlackUpdatedAt());
    }

    private void setUserImageFields(Message message, IngestionUserDTO user) {
        message.setAvatarHash(user.getAvatarHash());

        // Set both avatarUrl (preferred) and imageOriginal (legacy fallback)
        message.setAvatarUrl(user.getAvatarUrl());
        message.setImageOriginal(user.getImageOriginal());

        message.setImage24(user.getImage24());
        message.setImage32(user.getImage32());
        message.setImage48(user.getImage48());
        message.setImage72(user.getImage72());
        message.setImage192(user.getImage192());
        message.setImage512(user.getImage512());
        message.setImage1024(user.getImage1024());
    }

    // ============================================================================
    // ADMIN METHODS FOR SOFT-DELETED MESSAGES
    // ============================================================================

    /**
     * Gets all soft-deleted messages with pagination (admin only).
     * Returns messages where isDeleted=true.
     *
     * @param pageable The pagination information
     * @return Page of soft-deleted messages
     * @author vudu
     */
    public Page<Message> findDeletedMessages(Pageable pageable) {
        logger.debug("🔍 [ADMIN] Getting deleted messages with pagination: page={}, size={}",
                    pageable.getPageNumber(), pageable.getPageSize());

        try {
            // Get all messages from repository
            Iterable<Message> allMessages = messageRepository.findAll();
            List<Message> messageList = new ArrayList<>();
            allMessages.forEach(messageList::add);

            // Filter only soft-deleted messages
            messageList = messageList.stream()
                    .filter(m -> Boolean.TRUE.equals(m.getIsDeleted()))
                    .collect(java.util.stream.Collectors.toList());

            // Sort by deletedAt in descending order (most recently deleted first)
            messageList.sort((m1, m2) -> {
                if (m1.getDeletedAt() == null && m2.getDeletedAt() == null) return 0;
                if (m1.getDeletedAt() == null) return 1;
                if (m2.getDeletedAt() == null) return -1;
                return m2.getDeletedAt().compareTo(m1.getDeletedAt());
            });

            // Apply pagination manually
            int start = (int) pageable.getOffset();
            int end = Math.min(start + pageable.getPageSize(), messageList.size());

            if (start >= messageList.size()) {
                return Page.empty(pageable);
            }

            List<Message> pagedMessages = messageList.subList(start, end);

            logger.info("✅ [ADMIN] Found {} deleted messages (page {}/{})",
                       messageList.size(), pageable.getPageNumber() + 1,
                       (int) Math.ceil((double) messageList.size() / pageable.getPageSize()));

            return new org.springframework.data.domain.PageImpl<>(
                pagedMessages, pageable, messageList.size());

        } catch (Exception e) {
            logger.error("❌ [ADMIN] Error retrieving deleted messages: {}", e.getMessage(), e);
            return Page.empty(pageable);
        }
    }

    /**
     * Gets soft-deleted messages for a specific tenant with pagination (admin only).
     *
     * @param tenantId The tenant ID
     * @param pageable The pagination information
     * @return Page of soft-deleted messages for the tenant
     * @author vudu
     */
    public Page<Message> findDeletedMessagesByTenantId(String tenantId, Pageable pageable) {
        logger.debug("🔍 [ADMIN] Getting deleted messages for tenantId: {} with pagination", tenantId);

        try {
            // Use the repository method that filters deleted messages by tenant
            List<Message> messages = messageRepository.findDeletedMessagesByTenantId(tenantId, pageable);

            // Sort by deletedAt in descending order
            messages.sort((m1, m2) -> {
                if (m1.getDeletedAt() == null && m2.getDeletedAt() == null) return 0;
                if (m1.getDeletedAt() == null) return 1;
                if (m2.getDeletedAt() == null) return -1;
                return m2.getDeletedAt().compareTo(m1.getDeletedAt());
            });

            // Apply pagination manually
            int start = (int) pageable.getOffset();
            int end = Math.min(start + pageable.getPageSize(), messages.size());

            if (start >= messages.size()) {
                return Page.empty(pageable);
            }

            List<Message> pagedMessages = messages.subList(start, end);

            logger.info("✅ [ADMIN] Found {} deleted messages for tenant {} (page {}/{})",
                       messages.size(), tenantId, pageable.getPageNumber() + 1,
                       (int) Math.ceil((double) messages.size() / pageable.getPageSize()));

            return new org.springframework.data.domain.PageImpl<>(
                pagedMessages, pageable, messages.size());

        } catch (Exception e) {
            logger.error("❌ [ADMIN] Error retrieving deleted messages for tenant {}: {}",
                        tenantId, e.getMessage(), e);
            return Page.empty(pageable);
        }
    }

    /**
     * Gets deletion statistics including counts of deleted vs active messages.
     *
     * @return Map containing statistics
     * @author vudu
     */
    public Map<String, Object> getDeletionStatistics() {
        logger.debug("📊 [ADMIN] Generating deletion statistics");

        try {
            long activeCount = messageRepository.countByIsDeleted(false);
            long deletedCount = messageRepository.countByIsDeleted(true);
            long totalCount = activeCount + deletedCount;

            // Calculate oldest deletion
            List<Message> deletedMessages = messageRepository.findDeletedMessagesByTenantId(null,
                    org.springframework.data.domain.PageRequest.of(0, 1));
            Long oldestDeletionTime = null;
            if (!deletedMessages.isEmpty()) {
                oldestDeletionTime = deletedMessages.stream()
                        .map(Message::getDeletedAt)
                        .filter(java.util.Objects::nonNull)
                        .min(Long::compareTo)
                        .orElse(null);
            }

            Map<String, Object> stats = new HashMap<>();
            stats.put("totalMessages", totalCount);
            stats.put("activeMessages", activeCount);
            stats.put("deletedMessages", deletedCount);
            stats.put("deletionRate", totalCount > 0 ? (double) deletedCount / totalCount * 100 : 0.0);
            stats.put("oldestDeletionTimestamp", oldestDeletionTime);
            stats.put("timestamp", System.currentTimeMillis());

            logger.info("📊 [ADMIN] Deletion stats - Total: {} | Active: {} | Deleted: {} | Rate: {:.2f}%",
                       totalCount, activeCount, deletedCount, stats.get("deletionRate"));

            return stats;

        } catch (Exception e) {
            logger.error("❌ [ADMIN] Error generating deletion statistics: {}", e.getMessage(), e);
            return Map.of("error", e.getMessage());
        }
    }
}
