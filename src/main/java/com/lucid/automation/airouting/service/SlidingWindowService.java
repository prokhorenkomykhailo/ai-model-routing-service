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

    @Value("${sliding.window.max.tokens:15000}")
    private int maxTokensPerBatch;

    @Value("${sliding.window.avg.tokens.per.message:50}")
    private int avgTokensPerMessage;

    public SlidingWindowService(MessageRepository messageRepository, UserRepository userRepository) {
        this.messageRepository = messageRepository;
        this.userRepository = userRepository;
    }

    /**
     * Calculate optimal batch size based on token count estimation.
     * This method estimates the number of tokens in messages and calculates
     * an appropriate batch size to stay within the maxTokensPerBatch limit.
     *
     * @param messages List of messages to analyze
     * @param targetTokens Target number of tokens per batch
     * @return Optimal batch size
     */
    private int calculateTokenBasedBatchSize(List<Message> messages, int targetTokens) {
        if (messages == null || messages.isEmpty()) {
            logger.debug("🤷 No Smart Sizing: No messages provided for token-based batch size calculation, using default: {}", defaultBatchSize);
            return defaultBatchSize;
        }

        try {
            // Sample first few messages to estimate average token count
            int sampleSize = Math.min(10, messages.size());
            int totalEstimatedTokens = 0;

            logger.debug("🔬 Token Analysis: Examining {} sample messages to estimate batch size...", sampleSize);

            for (int i = 0; i < sampleSize; i++) {
                Message message = messages.get(i);
                int messageTokens = estimateTokenCount(message);
                totalEstimatedTokens += messageTokens;
            }

            // Calculate average tokens per message from sample
            int avgTokensFromSample = totalEstimatedTokens / sampleSize;

            // Use the higher of calculated average or configured minimum
            int effectiveAvgTokens = Math.max(avgTokensFromSample, avgTokensPerMessage);

            // Calculate optimal batch size based on token limit
            int tokenBasedBatchSize = targetTokens / effectiveAvgTokens;

            // Ensure batch size is within reasonable bounds (min 5, max 200)
            tokenBasedBatchSize = Math.max(5, Math.min(200, tokenBasedBatchSize));

            logger.info("🧠 Smart Batch Sizing: sample {} messages, avg {} tokens/message, target {} tokens → optimal batch size {}",
                        sampleSize, effectiveAvgTokens, targetTokens, tokenBasedBatchSize);

            return tokenBasedBatchSize;

        } catch (Exception e) {
            logger.warn("😱 Token Calculation Failed: Error calculating token-based batch size, falling back to default: {}", defaultBatchSize, e);
            return defaultBatchSize;
        }
    }

    /**
     * Estimate token count for a message.
     * This is a simple estimation based on text length and content.
     *
     * @param message The message to estimate tokens for
     * @return Estimated number of tokens
     */
    private int estimateTokenCount(Message message) {
        if (message == null) {
            return avgTokensPerMessage;
        }

        int tokenCount = 0;

        // Count tokens from message text (main content)
        if (message.getText() != null && !message.getText().trim().isEmpty()) {
            // Simple estimation: roughly 4 characters per token for English text
            tokenCount += message.getText().length() / 4;
        }

        // Add estimated tokens for user information
        tokenCount += estimateUserDataTokens(message);

        // Add base overhead for message metadata (timestamp, channel, etc.)
        tokenCount += 20;

        // Ensure minimum token count
        return Math.max(tokenCount, 10);
    }

    /**
     * Estimate token count for user data in a message.
     *
     * @param message The message with user data
     * @return Estimated tokens for user information
     */
    private int estimateUserDataTokens(Message message) {
        int userTokens = 0;

        // User names
        if (message.getName() != null) userTokens += message.getName().length() / 4;
        if (message.getDisplayName() != null) userTokens += message.getDisplayName().length() / 4;
        if (message.getFirstName() != null) userTokens += message.getFirstName().length() / 4;
        if (message.getLastName() != null) userTokens += message.getLastName().length() / 4;

        // User details
        if (message.getEmail() != null) userTokens += message.getEmail().length() / 4;
        if (message.getTitle() != null) userTokens += message.getTitle().length() / 4;
        if (message.getStatusText() != null) userTokens += message.getStatusText().length() / 4;

        // Base user metadata
        userTokens += 15;

        return userTokens;
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
            logger.warn("⚠️ Invalid Batch Size: maxNumberMessage is {}. Using default batch size: {}", maxMessage, defaultBatchSize);
            maxMessage = defaultBatchSize;
        }

        if (overlaping < 0 || overlaping > 100) {
            logger.warn("⚠️ Invalid Overlap: keepOverlaping percentage is {}%. Using default: {}%",
                       overlaping, defaultOverlapPercentage);
            overlaping = defaultOverlapPercentage;
        }

        if (callback == null) {
            logger.error("💥 Fatal Error: Cannot process messages - callbackFunction is null!");
            return 0;
        }

        try {
            // Load all messages for the workspace
            String deemergeUserId = workspace.getDeemergeUserId();
            String tenantId = workspace.getTenantId();
            List<Message> allMessages = loadMessagesForWorkspace(tenantId, deemergeUserId);

            if (allMessages.isEmpty()) {
                logger.info("📭 Empty Workspace: No messages found for workspace '{}' - time for coffee! ☕",
                           workspace.getDeemergeUserId());
                return 0;
            }

            // Check if there are any unprocessed messages
            long unprocessedCount = allMessages.stream()
                .filter(msg -> msg.getIsProcessed() == null || !msg.getIsProcessed())
                .count();
            if (unprocessedCount == 0) {
                logger.info("✨ All Caught Up: All {} messages processed for workspace '{}' - we're ahead of the game!",
                           allMessages.size(), workspace.getDeemergeUserId());
                logger.info("🚫 Sliding Window: Skipping processing - no unprocessed messages (we're so efficient! 🚀)");
                return 0;
            }

            // Calculate token-based batch size to prevent Kafka RecordTooLargeException
            int tokenBasedBatchSize = calculateTokenBasedBatchSize(allMessages, maxTokensPerBatch);

            // Use the smaller of the provided maxMessage or token-based batch size for safety
            int effectiveBatchSize = Math.min(maxMessage, tokenBasedBatchSize);

            logger.info("🧮 Batch Size Calculation: requested={}, token-based={}, effective={} (maxTokens={}) 🎯",
                       maxMessage, tokenBasedBatchSize, effectiveBatchSize, maxTokensPerBatch);

            // Messages are already sorted chronologically by loadMessagesForWorkspace method
            // Calculate overlap size, minimum of 20 messages or 20% of effective batch size
            int overlapSize = (effectiveBatchSize * overlaping) / 100;
            overlapSize = Math.max(overlapSize, 20); // Ensure at least 20 messages overlap

            logger.info("🔧 Processing Config: batchSize={}, overlapSize={}, unprocessedMessages={} 📊",
                       effectiveBatchSize, overlapSize, unprocessedCount);

            return processBatchesWithOverlap(allMessages, effectiveBatchSize, overlapSize, callback);

        } catch (Exception e) {
            logger.error("💥 Processing Failed: Error during sliding window processing for workspaceId: {} 😢", workspace, e);
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
            logger.info("🔍 Loading Messages: Searching for messages with deemergeUserId: {} 🕵️", deemergeUserId);

            // Load all messages for the workspace
            List<Message> messages = messageRepository.findByTenantIdAndDeemergeUserId(tenantId, deemergeUserId);

            if (messages.isEmpty()) {
                logger.info("📭 No Messages Found: Zero messages for deemergeUserId: {} (workspace might be quiet today 🤫)", deemergeUserId);
                return Collections.emptyList();
            }

            logger.info("🎉 Messages Loaded: Found {} messages for deemergeUserId: {} (jackpot! 💰)", messages.size(), deemergeUserId);

            // add user information to messages
            messages.forEach(message -> {
                if (message.getUserId() != null && !message.getUserId().trim().isEmpty()) {
                    // Assuming UserData is a class that contains user information
                    String slackUserId = message.getUserId();
                    if (slackUserId != null && !slackUserId.trim().isEmpty()) {
                        List<User> userList = userRepository.findBySlackUserId(slackUserId);
                        if (userList.isEmpty()) {
                            logger.warn("😴 Missing User Data: No user data found for slackUserId: {} (user might be a ghost 👻)", slackUserId);
                            return;
                        }
                        User userData = userList.get(0);
                        message = upateMessageWithUserData(message, userData);
                        // logger.debug("Loaded user data for message: {}", slackUserId);
                    } else {
                        logger.warn("🆔 Empty User ID: Message with ID {} has empty userId (anonymous message? 🕵️)", message.getId());
                    }
                }
            });

            // Sort messages chronologically by messageTs (oldest first, newest last)
            messages.sort(Comparator.comparing(Message::getMessageTs));

            logger.info("⏰ Messages Sorted: {} messages arranged chronologically (oldest to newest) ⏳", messages.size());

            return messages;

        } catch (Exception e) {
            logger.error("💥 Load Failed: Error loading messages for deemergeUserId: {} 😭", deemergeUserId, e);
            return Collections.emptyList();
        }
    }

    private Message upateMessageWithUserData(Message message, User userData) {
        if (userData == null) {
            logger.warn("👤 Null User Data: User data is null for message ID: {} (mysterious user 🤔)", message.getId());
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
            logger.error("💥 User Data Update Failed: Error updating message {} with user data for slackUserId: {} 😢",
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
            logger.info("📭 Empty Processing Queue: No messages to process (time for a break! 🍪)");
            return 0;
        }

        int totalProcessed = 0;
        int batchNumber = 1;
        int startIndex = 0;

        logger.info("🚀 Starting Batch Processing: Ready to process {} messages in sliding window batches! 💪", allMessages.size());

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

            logger.info("📦 Batch {} Analysis: Processing messages [{} to {}] - {} total ({} already processed, {} new) 🔍",
                        batchNumber, startIndex + 1, endIndex, currentBatch.size(), processedInBatch, unprocessedInBatch);

            // Only process the batch if it has at least one unprocessed message
            if (unprocessedInBatch > 0) {
                try {
                    logger.info("⚡ Processing Batch {}: Starting AI enrichment for {} messages... 🤖", batchNumber, unprocessedInBatch);

                    // Process the batch using the callback function
                    callbackFunction.apply(currentBatch);

                    // Mark all messages in this batch as processed
                    markMessagesAsProcessed(currentBatch);

                    totalProcessed += currentBatch.size();

                    logger.info("✅ Batch {} Success: Processed {} messages ({} were new, {} were already processed) 🎉",
                               batchNumber, currentBatch.size(), unprocessedInBatch, processedInBatch);

                    // Immediately clean up this batch after successful processing
                    cleanupBatchMessages(currentBatch, overlapSize, batchNumber);

                } catch (Exception e) {
                    logger.error("💥 Batch {} Failed: Error processing messages [{}-{}] 😱",
                               batchNumber, startIndex, endIndex - 1, e);
                    // Continue processing other batches even if one fails
                }
            } else {
                logger.info("⏭️  Batch {} Skipped: All {} messages already processed - no new messages to process (efficient! ✨)",
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

        logger.info("🎯 Sliding Window Complete: Processed {} batches and handled {} total messages (Mission accomplished! 🏆)",
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
            logger.debug("🧹 Nothing to Clean: No messages to cleanup for batch {} (already spotless! ✨)", batchNumber);
            return;
        }

        int totalMessages = batchMessages.size();
        int messagesToKeep = Math.min(overlapSize, totalMessages);
        int messagesToDelete = totalMessages - messagesToKeep;

        if (messagesToDelete <= 0) {
            logger.debug("🔄 Keeping All: Batch {}: No messages to delete. Total: {}, Keeping: {} for overlap (all precious! 💎)",
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

            logger.info("🗑️ Cleanup Complete: Batch {}: Cleaned up {} processed messages, keeping {} for overlap (tidying up! 🧽)",
                       batchNumber, messagesToDelete, messagesToKeep);

        } catch (Exception e) {
            logger.error("💥 Cleanup Failed: Error cleaning up batch {} messages: {} 😰", batchNumber, e.getMessage(), e);
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
            logger.info("🧹 Workspace Cleanup: Starting cleanup for workspaceId: {}, keeping {} recent messages 🏠",
                       workspace.getId(), keepRecentCount);
            String tenanntId = workspace.getTenantId();
            String workspaceId = workspace.getId();
            String deemergeUserId = workspace.getDeemergeUserId();
            List<Message> allMessages = loadMessagesForWorkspace(tenanntId, deemergeUserId);

            if (allMessages.isEmpty()) {
                logger.info("📭 Clean Workspace: No messages found for cleanup in workspaceId: {} (already spotless! ✨)", workspaceId);
                return 0;
            }

            int totalMessages = allMessages.size();
            if (totalMessages <= keepRecentCount) {
                logger.info("💎 Keeping All: Workspace {} has {} messages, keeping all (requested to keep {}) - all precious! 💰",
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
            logger.info("🗑️ Cleanup Success: Deleted {} old messages, kept {} recent ones (workspace is now tidy! 🧹)",
                       deletedCount, keepRecentCount);
            return deletedCount;

        } catch (Exception e) {
            logger.error("💥 Cleanup Failed: Error during workspace cleanup for workspaceId: {} 😭", workspace.getId(), e);
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
            logger.debug("✅ Nothing to Mark: No messages to mark as processed (already done! 🎯)");
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
                logger.debug("✅ All Already Marked: No unprocessed messages to mark as processed in this batch (we're efficient! 🚀)");
                return;
            }

            // Save all updated messages back to Redis
            messageRepository.saveAll(updatedMessages);

            logger.info("✅ Processing Status Updated: Marked {} messages as processed (stamped and approved! 📋)", updatedMessages.size());

        } catch (Exception e) {
            logger.error("💥 Status Update Failed: Error marking messages as processed: {} 😰", e.getMessage(), e);
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
