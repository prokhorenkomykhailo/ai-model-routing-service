package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.model.Message;
import com.lucid.automation.airouting.model.User;
import com.lucid.automation.airouting.model.Workspace;
import com.lucid.automation.airouting.repository.MessageRepository;
import com.lucid.automation.airouting.repository.UserRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Service for processing messages using sliding window approach.
 * This service handles workspace-based message processing with configurable batch sizes and overlap.
 */
@Service
public class SlidingWindowService {

    private static final Logger logger = LoggerFactory.getLogger(SlidingWindowService.class);

    private final MessageRepository messageRepository;
    private final UserRepository userRepository;

    @Value("${sliding.window.default.batch.size:50}")
    private int defaultBatchSize;

    @Value("${sliding.window.default.overlap.percentage:20}")
    private int defaultOverlapPercentage;

    public SlidingWindowService(MessageRepository messageRepository, UserRepository userRepository) {
        this.messageRepository = messageRepository;
        this.userRepository = userRepository;
    }

    /**
     * Process messages for a workspace using sliding window approach.
     * Messages are loaded from Redis (all messages), sorted chronologically, and processed in batches with overlap.
     * Only processes if there's at least one unprocessed message. After processing, marks all processed messages with isProcessed=true
     * and deletes all processed messages keeping only the latest N messages (where N = overlap size).
     * If all messages are already marked as processed, the sliding window will not run.
     *
     * @param workspace The workspace ID to process messages for
     * @param maxMessage Maximum number of messages to process per batch
     * @param overlaping Percentage of messages to keep as overlap between batches (0-100)
     * @param callback Function to process each batch of messages
     * @return Total number of messages processed
     */
    public int processMessages(Workspace workspace, int maxMessage, int overlaping, Function<List<Message>, Void> callback) {

        if (maxMessage <= 0) {
            logger.warn("Invalid maxNumberMessage: {}. Using default batch size: {}", maxMessage, defaultBatchSize);
            maxMessage = defaultBatchSize;
        }

        if (overlaping < 0 || overlaping > 100) {
            logger.warn("Invalid keepOverlaping percentage: {}. Using default: {}",
                       overlaping, defaultOverlapPercentage);
            overlaping = defaultOverlapPercentage;
        }

        if (callback == null) {
            logger.warn("Cannot process messages: callbackFunction is null");
            return 0;
        }

        try {
            // Load all messages for the workspace
            String deemergeUserId = workspace.getDeemergeUserId();
            String tenantId = workspace.getTenantId();
            List<Message> allMessages = loadMessagesForWorkspace(tenantId, deemergeUserId);

            if (allMessages.isEmpty()) {
                logger.info("📭 No Messages: No messages found for workspace '{}' - nothing to process",
                           workspace.getDeemergeUserId());
                return 0;
            }

            // Check if there are any unprocessed messages
            long unprocessedCount = allMessages.stream()
                .filter(msg -> msg.getIsProcessed() == null || !msg.getIsProcessed())
                .count();
            if (unprocessedCount == 0) {
                logger.info("📋 Workspace Analysis: All {} messages have been processed - no new messages to process for workspace '{}'",
                           allMessages.size(), workspace.getDeemergeUserId());
                logger.info("🚫 Sliding Window: Skipping processing - no unprocessed messages found");
                return 0;
            }

            // Messages are already sorted chronologically by loadMessagesForWorkspace method
            // Calculate overlap size, minimum of 20 messages or 20% of maxMessage
            int overlapSize = (maxMessage * overlaping) / 100;
            overlapSize = Math.max(overlapSize, 20); // Ensure at least 20 messages overlap

            logger.debug("Processing with batchSize: {}, overlapSize: {}", maxMessage, overlapSize);

            return processBatchesWithOverlap(allMessages, maxMessage, overlapSize, callback);

        } catch (Exception e) {
            logger.error("Error during sliding window processing for workspaceId: {}", workspace, e);
            return 0;
        }
    }

    /**
     * Load all messages for a specific workspace from Redis.
     *
     * @param deemergeUserId The demerge user ID
     * @return List of messages sorted chronologically (oldest first, newest last)
     */
    private List<Message> loadMessagesForWorkspace(String tenantId, String deemergeUserId) {
        try {
            logger.debug("Loading messages for deemergeUserId: {}", deemergeUserId);

            // Load all messages for the workspace
            List<Message> messages = messageRepository.findByTenantIdAndDeemergeUserId(tenantId, deemergeUserId);

            if (messages.isEmpty()) {
                logger.info("No messages found for deemergeUserId: {}", deemergeUserId);
                return Collections.emptyList();
            }

            logger.info("Found {} messages for deemergeUserId: {}", messages.size(), deemergeUserId);

            // add user information to messages
            messages.forEach(message -> {
                if (message.getUserId() != null && !message.getUserId().trim().isEmpty()) {
                    // Assuming UserData is a class that contains user information
                    String slackUserId = message.getUserId();
                    if (slackUserId != null && !slackUserId.trim().isEmpty()) {
                        List<User> userList = userRepository.findBySlackUserId(slackUserId);
                        if (userList.isEmpty()) {
                            logger.warn("No user data found for slackUserId: {}", slackUserId);
                            return;
                        }
                        User userData = userList.get(0);
                        message = upateMessageWithUserData(message, userData);
                        // logger.debug("Loaded user data for message: {}", slackUserId);
                    } else {
                        logger.warn("Message with ID {} has empty userId", message.getId());
                    }
                }
            });

            // Sort messages chronologically by messageTs (oldest first, newest last)
            messages.sort(Comparator.comparing(Message::getMessageTs));

            logger.debug("Found and sorted {} messages chronologically", messages.size());

            return messages;

        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    private Message upateMessageWithUserData(Message message, User userData) {
        if (userData == null) {
            logger.warn("User data is null for message ID: {}", message.getId());
            return message;
        }

        try {
            // Basic user identification
            message.setSlackUserId(userData.getSlackUserId());
            message.setTeamId(userData.getTeamId());
            message.setTeamName(userData.getTeamName());

            // User names and display information
            message.setName(userData.getName());
            message.setDisplayName(userData.getDisplayName());
            message.setDisplayNameNormalized(userData.getDisplayNameNormalized());
            message.setRealNameNormalized(userData.getRealNameNormalized());
            message.setFirstName(userData.getFirstName());
            message.setLastName(userData.getLastName());

            // Contact information
            message.setEmail(userData.getEmail());
            message.setEmailConfirmed(userData.getEmailConfirmed());
            message.setPhone(userData.getPhone());
            message.setTitle(userData.getTitle());
            message.setPronouns(userData.getPronouns());

            // Status and avatar information
            message.setStatusText(userData.getStatusText());
            message.setAvatarHash(userData.getAvatarHash());

            // Profile images (all sizes)
            message.setImageOriginal(userData.getImageOriginal());
            message.setImage24(userData.getImage24());
            message.setImage32(userData.getImage32());
            message.setImage48(userData.getImage48());
            message.setImage72(userData.getImage72());
            message.setImage192(userData.getImage192());
            message.setImage512(userData.getImage512());
            message.setImage1024(userData.getImage1024());

            // Slack metadata
            message.setSlackUpdatedAt(userData.getSlackUpdatedAt());
            return message;
        } catch (Exception e) {
            logger.error("Error updating message {} with user data for slackUserId: {}",
                        message.getId(), userData.getSlackUserId(), e);
            return message;
        }
    }

    /**
     * Process messages in batches with sliding window overlap.
     * After processing each batch, immediately deletes the processed messages from Redis,
     * keeping only the overlap messages for the next batch.
     *
     * @param allMessages All messages to process
     * @param batchSize Size of each batch
     * @param overlapSize Number of latest messages to keep after processing each batch
     * @param callbackFunction Function to process each batch
     * @return Total number of messages processed
     */
    private int processBatchesWithOverlap(List<Message> allMessages, int batchSize, int overlapSize,
                                         Function<List<Message>, Void> callbackFunction) {

        if (allMessages.isEmpty()) {
            return 0;
        }

        int totalProcessed = 0;
        int batchNumber = 1;
        int startIndex = 0;

        while (startIndex < allMessages.size()) {
            // Calculate end index for current batch
            int endIndex = Math.min(startIndex + batchSize, allMessages.size());

            // Extract current batch
            List<Message> currentBatch = new ArrayList<>(allMessages.subList(startIndex, endIndex));

            // Check if this batch has at least one unprocessed message
            long unprocessedInBatch = currentBatch.stream()
                .filter(msg -> msg.getIsProcessed() == null || !msg.getIsProcessed())
                .count();
            long processedInBatch = currentBatch.size() - unprocessedInBatch;

            logger.info("📦 Batch {} Analysis: Processing messages [{} to {}] - {} total ({} already processed, {} new)",
                        batchNumber, startIndex + 1, endIndex, currentBatch.size(), processedInBatch, unprocessedInBatch);

            // Only process the batch if it has at least one unprocessed message
            if (unprocessedInBatch > 0) {
                try {
                    // Process the batch using the callback function
                    callbackFunction.apply(currentBatch);

                    // Mark all messages in this batch as processed
                    markMessagesAsProcessed(currentBatch);

                    totalProcessed += currentBatch.size();

                    logger.info("✅ Batch {} Success: Processed {} messages ({} were new, {} were already processed)",
                               batchNumber, currentBatch.size(), unprocessedInBatch, processedInBatch);

                    // Immediately clean up this batch after successful processing
                    cleanupBatchMessages(currentBatch, overlapSize, batchNumber);

                } catch (Exception e) {
                    logger.error("Error processing batch {} for messages [{}-{}]",
                               batchNumber, startIndex, endIndex - 1, e);
                    // Continue processing other batches even if one fails
                }
            } else {
                logger.info("⏭️  Batch {} Skipped: All {} messages already processed - no new messages to process",
                           batchNumber, currentBatch.size());
            }

            // Calculate next start index with overlap
            // If this is the last batch, we're done
            if (endIndex >= allMessages.size()) {
                break;
            }

            // Move start index forward, accounting for overlap
            startIndex = endIndex - overlapSize;

            // Ensure we don't go backwards
            if (startIndex <= 0 || startIndex >= endIndex) {
                startIndex = endIndex;
            }

            batchNumber++;
        }

        logger.info("🎯 Sliding Window Complete: Processed {} batches and handled {} total messages",
                   batchNumber - 1, totalProcessed);

        return totalProcessed;
    }

    /**
     * Clean up messages from a processed batch, keeping only the overlap messages.
     * This method deletes messages that are not part of the overlap for the next batch.
     *
     * @param batchMessages The messages from the processed batch
     * @param overlapSize Number of latest messages to keep for overlap
     * @param batchNumber The batch number being processed (for logging)
     */
    private void cleanupBatchMessages(List<Message> batchMessages, int overlapSize, int batchNumber) {
        if (batchMessages == null || batchMessages.isEmpty()) {
            logger.debug("No messages to cleanup for batch {}", batchNumber);
            return;
        }

        int totalMessages = batchMessages.size();
        int messagesToKeep = Math.min(overlapSize, totalMessages);
        int messagesToDelete = totalMessages - messagesToKeep;

        if (messagesToDelete <= 0) {
            logger.debug("Batch {}: No messages to delete. Total: {}, Keeping: {} for overlap",
                       batchNumber, totalMessages, messagesToKeep);
            return;
        }

        try {
            // Messages are already sorted chronologically (oldest first) by loadMessagesForWorkspace
            // Get IDs of oldest messages to delete, keeping newest for overlap
            List<String> messageIdsToDelete = batchMessages.stream()
                    .limit(messagesToDelete) // take only the oldest messages to delete
                    .map(Message::getId)
                    .collect(Collectors.toList());

            // Delete messages from Redis
            messageRepository.deleteAllById(messageIdsToDelete);

            logger.debug("Batch {}: Cleaned up {} processed messages, keeping {} for overlap",
                       batchNumber, messagesToDelete, messagesToKeep);

        } catch (Exception e) {
            logger.error("Error cleaning up batch {} messages: {}", batchNumber, e.getMessage(), e);
            // Don't throw exception - let processing continue even if cleanup fails
        }
    }

    /**
     * Manually clean up all processed messages for a workspace, keeping only the most recent messages.
     * This can be used for maintenance or when you want to clean up old processed messages.
     *
     * @param workspaceId The workspace ID to clean up
     * @param keepRecentCount Number of most recent messages to keep (0 to delete all)
     * @return Number of messages deleted
     */
    public int cleanupWorkspace(Workspace workspace, int keepRecentCount) {
        keepRecentCount = Math.max(0, keepRecentCount); // Ensure non-negative

        try {
            logger.info("Starting cleanup for workspaceId: {}, keeping {} recent messages",
                       workspace.getId(), keepRecentCount);
            String tenanntId = workspace.getTenantId();
            String workspaceId = workspace.getId();
            String deemergeUserId = workspace.getDeemergeUserId();
            List<Message> allMessages = loadMessagesForWorkspace(tenanntId, deemergeUserId);

            if (allMessages.isEmpty()) {
                logger.info("No messages found for cleanup in workspaceId: {}", workspaceId);
                return 0;
            }

            int totalMessages = allMessages.size();
            if (totalMessages <= keepRecentCount) {
                logger.info("Workspace {} has {} messages, keeping all (requested to keep {})",
                           workspaceId, totalMessages, keepRecentCount);
                return 0;
            }

            // Messages are already sorted (oldest first), reverse to get newest first
            Collections.reverse(allMessages);

            // Get messages to delete (skip the first keepRecentCount messages)
            List<String> messageIdsToDelete = allMessages.stream()
                    .skip(keepRecentCount)
                    .map(Message::getId)
                    .collect(Collectors.toList());

            // Delete messages from Redis
            messageRepository.deleteAllById(messageIdsToDelete);

            int deletedCount = messageIdsToDelete.size();
            return deletedCount;

        } catch (Exception e) {
            logger.error("Error during workspace cleanup for workspaceId: {}", workspace.getId(), e);
            return 0;
        }
    }

    /**
     * Mark a list of messages as processed by setting isProcessed to true.
     * Only updates messages that are currently unprocessed (null or false).
     *
     * @param messages The list of messages to mark as processed
     */
    private void markMessagesAsProcessed(List<Message> messages) {
        if (messages == null || messages.isEmpty()) {
            logger.debug("No messages to mark as processed");
            return;
        }

        try {
            List<Message> updatedMessages = new ArrayList<>();

            for (Message message : messages) {
                // Only mark as processed if it's currently unprocessed
                if (message.getIsProcessed() == null || !message.getIsProcessed()) {
                    message.setIsProcessed(true);
                    updatedMessages.add(message);
                }
            }

            if (updatedMessages.isEmpty()) {
                logger.debug("No unprocessed messages to mark as processed in this batch");
                return;
            }

            // Save all updated messages back to Redis
            messageRepository.saveAll(updatedMessages);

            logger.debug("Marked {} messages as processed", updatedMessages.size());

        } catch (Exception e) {
            logger.error("Error marking messages as processed: {}", e.getMessage(), e);
            // Don't throw exception - let processing continue even if marking fails
        }
    }

    /**
     * Statistics for workspace messages
     */
    public static class WorkspaceMessageStats {
        private final String workspaceId;
        private final long totalMessages;
        private final long uniqueChannels;
        private final long uniqueUsers;

        public WorkspaceMessageStats(String workspaceId, long totalMessages,
                                   long uniqueChannels, long uniqueUsers) {
            this.workspaceId = workspaceId;
            this.totalMessages = totalMessages;
            this.uniqueChannels = uniqueChannels;
            this.uniqueUsers = uniqueUsers;
        }

        public String getWorkspaceId() { return workspaceId; }
        public long getTotalMessages() { return totalMessages; }
        public long getUniqueChannels() { return uniqueChannels; }
        public long getUniqueUsers() { return uniqueUsers; }

        @Override
        public String toString() {
            return String.format("WorkspaceMessageStats{workspaceId='%s', totalMessages=%d, uniqueChannels=%d, uniqueUsers=%d}",
                               workspaceId, totalMessages, uniqueChannels, uniqueUsers);
        }
    }
}
