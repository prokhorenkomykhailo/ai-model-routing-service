package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.model.Message;
import com.lucid.automation.airouting.model.User;
import com.lucid.automation.airouting.model.Workspace;
import com.lucid.automation.airouting.repository.MessageRepository;
import com.lucid.automation.airouting.repository.UserRepository;
import com.lucid.automation.airouting.repository.WorkspaceRepository;
import com.lucid.automation.airouting.util.TenantValidationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private final WorkspaceRepository workspaceRepository;

    @Value("${sliding.window.default.batch.size:50}")
    private int defaultBatchSize;

    @Value("${sliding.window.default.overlap.percentage:20}")
    private int defaultOverlapPercentage;

    @Value("${sliding.window.max.tokens:15000}")
    private int maxTokensPerBatch;

    @Value("${sliding.window.avg.tokens.per.message:50}")
    private int avgTokensPerMessage;

    @Value("${sliding.window.min.new.messages:5}")
    private int minNewMessages;

    @Value("${sliding.window.max.wait.hours:4}")
    private int maxWaitHours;

    public SlidingWindowService(MessageRepository messageRepository, UserRepository userRepository, WorkspaceRepository workspaceRepository) {
        this.messageRepository = messageRepository;
        this.userRepository = userRepository;
        this.workspaceRepository = workspaceRepository;
    }

    /**
     * Determines if a workspace should be processed based on:
     * 1. Invalid tenant ID check (skip processing if invalid)
     * 2. Number of new messages (≥5 messages)
     * 3. Time since last processing (≥4 hours)
     *
     * @param workspace The workspace to evaluate
     * @return ProcessingDecision with decision and reason
     */
    public ProcessingDecision shouldProcessWorkspace(Workspace workspace) {
        String workspaceId = workspace.getId();
        String tenantId = workspace.getTenantId();

        // First, validate tenant ID - reject invalid tenants consistently across the project
        if (TenantValidationUtil.isInvalidTenantId(tenantId)) {
            String reason = TenantValidationUtil.getInvalidTenantIdReason(tenantId);
            logger.warn("🚫 SLIDING-WINDOW: Skipping workspace [{}] due to invalid tenant ID [{}]: {}",
                       workspaceId, tenantId, reason);
            return new ProcessingDecision(false, ProcessingDecision.Reason.INVALID_TENANT,
                "Invalid tenant ID: " + reason);
        }

        try {
            // Count unprocessed messages for this workspace
            String deemergeUserId = workspace.getDeemergeUserId();
            List<Message> allMessages = loadMessagesForWorkspace(tenantId, deemergeUserId);

            long unprocessedCount = allMessages.stream()
                .filter(msg -> msg.getIsProcessed() == null || !msg.getIsProcessed())
                .count();

            // Update workspace unprocessed message count
            workspace.setUnprocessedMessageCount(unprocessedCount);

            // Check if we have enough new messages
            if (unprocessedCount >= minNewMessages) {
                logger.info("✅ SLIDING-WINDOW: Workspace [{}] has {} unprocessed messages (≥{}) - processing immediately",
                           workspaceId, unprocessedCount, minNewMessages);
                return new ProcessingDecision(true, ProcessingDecision.Reason.SUFFICIENT_MESSAGES,
                    String.format("%d unprocessed messages (≥%d required)", unprocessedCount, minNewMessages));
            }

            // Check time-based processing
            Instant lastProcessed = workspace.getLastProcessedAt();
            if (lastProcessed == null) {
                logger.info("✅ SLIDING-WINDOW: Workspace [{}] has never been processed - processing now", workspaceId);
                return new ProcessingDecision(true, ProcessingDecision.Reason.NEVER_PROCESSED,
                    "Workspace has never been processed");
            }

            Duration timeSinceLastProcessed = Duration.between(lastProcessed, Instant.now());
            long hoursSinceLastProcessed = timeSinceLastProcessed.toHours();

            if (hoursSinceLastProcessed >= maxWaitHours) {
                logger.info("✅ SLIDING-WINDOW: Workspace [{}] last processed {} hours ago (≥{} hours) - processing due to time threshold",
                           workspaceId, hoursSinceLastProcessed, maxWaitHours);
                return new ProcessingDecision(true, ProcessingDecision.Reason.TIME_THRESHOLD,
                    String.format("Last processed %d hours ago (≥%d hours required)", hoursSinceLastProcessed, maxWaitHours));
            }

            // Not enough messages and not enough time passed
            logger.info("⏸️  SLIDING-WINDOW: Workspace [{}] skipped - only {} unprocessed messages (<{}) and {} hours since last processing (<{})",
                       workspaceId, unprocessedCount, minNewMessages, hoursSinceLastProcessed, maxWaitHours);
            return new ProcessingDecision(false, ProcessingDecision.Reason.INSUFFICIENT_CRITERIA,
                String.format("Only %d unprocessed messages (<%d required) and %d hours since last processing (<%d hours required)",
                             unprocessedCount, minNewMessages, hoursSinceLastProcessed, maxWaitHours));

        } catch (Exception e) {
            logger.error("🚨 SLIDING-WINDOW: Error evaluating workspace [{}]: {}", workspaceId, e.getMessage(), e);
            return new ProcessingDecision(false, ProcessingDecision.Reason.ERROR,
                "Error evaluating workspace: " + e.getMessage());
        }
    }

    /**
     * Data class representing a processing decision for a workspace
     */
    public static class ProcessingDecision {
        private final boolean shouldProcess;
        private final Reason reason;
        private final String description;

        public ProcessingDecision(boolean shouldProcess, Reason reason, String description) {
            this.shouldProcess = shouldProcess;
            this.reason = reason;
            this.description = description;
        }

        public boolean shouldProcess() { return shouldProcess; }
        public Reason getReason() { return reason; }
        public String getDescription() { return description; }

        public enum Reason {
            SUFFICIENT_MESSAGES,
            TIME_THRESHOLD,
            NEVER_PROCESSED,
            INSUFFICIENT_CRITERIA,
            INVALID_TENANT,
            ERROR
        }

        @Override
        public String toString() {
            return String.format("ProcessingDecision{shouldProcess=%s, reason=%s, description='%s'}",
                               shouldProcess, reason, description);
        }
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
        tokenCount += estimateTextTokens(message.getText());

        // Add estimated tokens for user information
        tokenCount += estimateUserDataTokens(message);

        // Add base overhead for message metadata (timestamp, channel, etc.)
        tokenCount += 20;

        // Ensure minimum token count
        return Math.max(tokenCount, 10);
    }

    /**
     * Utility method to estimate tokens from text.
     *
     * @param text The text to estimate tokens for
     * @return Estimated number of tokens
     */
    private int estimateTextTokens(String text) {
        if (text == null || text.trim().isEmpty()) {
            return 0;
        }
        // Simple estimation: roughly 4 characters per token for English text
        return text.length() / 4;
    }

    /**
     * Estimate token count for user data in a message.
     *
     * @param message The message with user data
     * @return Estimated tokens for user information
     */
    private int estimateUserDataTokens(Message message) {
        // Use utility method to reduce repetitive pattern
        int userTokens = 0;
        userTokens += estimateTextTokens(message.getName());
        userTokens += estimateTextTokens(message.getDisplayName());
        userTokens += estimateTextTokens(message.getFirstName());
        userTokens += estimateTextTokens(message.getLastName());
        userTokens += estimateTextTokens(message.getEmail());
        userTokens += estimateTextTokens(message.getTitle());
        userTokens += estimateTextTokens(message.getStatusText());

        // Base user metadata
        userTokens += 15;

        return userTokens;
    }

    /**
     * Validate and fix processing parameters.
     *
     * @param maxMessage The requested batch size
     * @param overlaping The requested overlap percentage (ignored, using 80/20 strategy)
     * @param callback The callback function
     * @return ValidatedParams object with corrected values, or null if callback is null
     */
    private ValidatedParams validateProcessingParameters(int maxMessage, int overlaping, Function<List<Message>, Void> callback) {
        if (callback == null) {
            logger.error("💥 Fatal Error: Cannot process messages - callbackFunction is null!");
            return null;
        }

        int validBatchSize = maxMessage;
        if (maxMessage <= 0) {
            logger.warn("⚠️ Invalid Batch Size: maxNumberMessage is {}. Using default batch size: {}", maxMessage, defaultBatchSize);
            validBatchSize = defaultBatchSize;
        }

        // Note: overlaping parameter is ignored - we now use 80/20 strategy
        if (overlaping != defaultOverlapPercentage) {
            logger.info("ℹ️  Overlap Strategy: Using 80/20 strategy instead of {}% (new messages: 80%, old messages: 20%)", overlaping);
        }

        return new ValidatedParams(validBatchSize);
    }

    /**
     * Simple data class to hold validated parameters.
     */
    private static class ValidatedParams {
        final int batchSize;

        ValidatedParams(int batchSize) {
            this.batchSize = batchSize;
        }
    }

    /**
     * Process messages for a workspace using sliding window approach.
     * Enhanced version that only processes if:
     * 1. Tenant ID is valid (consistent with project-wide tenant validation)
     * 2. There are at least 5 new (unprocessed) messages OR
     * 3. 4 hours have passed since last processing
     *
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

        // Check if workspace should be processed based on new criteria
        ProcessingDecision decision = shouldProcessWorkspace(workspace);
        if (!decision.shouldProcess()) {
            logger.info("🚫 SLIDING-WINDOW: Skipping workspace [{}] - {}",
                       workspace.getId(), decision.getDescription());
            return 0;
        }

        logger.info("🚀 SLIDING-WINDOW: Processing workspace [{}] - {}",
                   workspace.getId(), decision.getDescription());

        // Validate and fix parameters
        ValidatedParams params = validateProcessingParameters(maxMessage, overlaping, callback);
        if (params == null) {
            return 0; // callback was null
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

            // Check if there are any unprocessed messages (double-check after shouldProcessWorkspace)
            long unprocessedCount = allMessages.stream()
                .filter(msg -> msg.getIsProcessed() == null || !msg.getIsProcessed())
                .count();
            if (unprocessedCount == 0) {
                logger.info("✨ All Caught Up: All {} messages processed for workspace '{}' - we're ahead of the game!",
                           allMessages.size(), workspace.getDeemergeUserId());
                logger.info("🚫 Sliding Window: Skipping processing - no unprocessed messages (we're so efficient! 🚀)");
                updateWorkspaceProcessingStatus(workspace, allMessages.size(), 0);
                return 0;
            }

            // Calculate token-based batch size to prevent Kafka RecordTooLargeException
            int tokenBasedBatchSize = calculateTokenBasedBatchSize(allMessages, maxTokensPerBatch);

            // Use the smaller of the validated maxMessage or token-based batch size for safety
            int effectiveBatchSize = Math.min(params.batchSize, tokenBasedBatchSize);

            logger.info("🧮 Batch Size Calculation: requested={}, token-based={}, effective={} (maxTokens={}) 🎯",
                       params.batchSize, tokenBasedBatchSize, effectiveBatchSize, maxTokensPerBatch);

            // Messages are already sorted chronologically by loadMessagesForWorkspace method
            // Calculate overlap size using 80/20 strategy: 80% new messages + 20% old messages
            int overlapSize = calculateOptimalOverlapSize(effectiveBatchSize, unprocessedCount);

            logger.info("🔧 Processing Config: batchSize={}, overlapSize={} (80/20 strategy), unprocessedMessages={} 📊",
                       effectiveBatchSize, overlapSize, unprocessedCount);

            int totalProcessed = processBatchesWithOverlap(allMessages, effectiveBatchSize, overlapSize, callback);

            // Update workspace processing status after successful processing
            updateWorkspaceProcessingStatus(workspace, allMessages.size(), totalProcessed);

            return totalProcessed;

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
            enrichMessagesWithUserData(messages);

            // Sort messages chronologically by messageTs (oldest first, newest last)
            messages.sort(Comparator.comparing(Message::getMessageTs));

            logger.info("⏰ Messages Sorted: {} messages arranged chronologically (oldest to newest) ⏳", messages.size());

            return messages;

        } catch (Exception e) {
            logger.error("💥 Load Failed: Error loading messages for deemergeUserId: {} 😭", deemergeUserId, e);
            return Collections.emptyList();
        }
    }

    /**
     * Enrich messages with user data in a batch operation.
     * This method simplifies user data handling by processing all messages at once.
     *
     * @param messages List of messages to enrich with user data
     */
    private void enrichMessagesWithUserData(List<Message> messages) {
        if (messages == null || messages.isEmpty()) {
            return;
        }

        // Get unique user IDs that need enrichment
        Set<String> userIdsToLookup = messages.stream()
            .map(Message::getUserId)
            .filter(userId -> userId != null && !userId.trim().isEmpty())
            .collect(Collectors.toSet());

        if (userIdsToLookup.isEmpty()) {
            logger.debug("🆔 No Valid User IDs: No user IDs found for enrichment (all anonymous? 🕵️)");
            return;
        }

        logger.debug("👥 User Lookup: Fetching data for {} unique users", userIdsToLookup.size());

        // Create a map of userId -> User for quick lookup
        Map<String, User> userDataMap = new HashMap<>();
        for (String slackUserId : userIdsToLookup) {
            List<User> userList = userRepository.findBySlackUserId(slackUserId);
            if (!userList.isEmpty()) {
                userDataMap.put(slackUserId, userList.get(0));
            } else {
                logger.warn("😴 Missing User Data: No user data found for slackUserId: {} (user might be a ghost 👻)", slackUserId);
            }
        }

        // Enrich messages with user data
        int enrichedCount = 0;
        for (Message message : messages) {
            String userId = message.getUserId();
            if (userId != null && !userId.trim().isEmpty()) {
                User userData = userDataMap.get(userId);
                if (userData != null) {
                    copyUserDataToMessage(message, userData);
                    enrichedCount++;
                }
            }
        }

        logger.debug("✨ User Enrichment: Enhanced {} messages with user data (teamwork! 🤝)", enrichedCount);
    }

    /**
     * Copy user data fields to message. Simplified version that focuses on essential fields.
     *
     * @param message The message to enrich
     * @param userData The user data to copy from
     */
    private void copyUserDataToMessage(Message message, User userData) {
        if (userData == null) {
            return;
        }

        try {
            // Essential user identification
            message.setSlackUserId(userData.getSlackUserId());
            message.setTeamId(userData.getTeamId());
            message.setTeamName(userData.getTeamName());

            // Primary names (simplified - only the most commonly used ones)
            message.setName(userData.getName());
            message.setDisplayName(userData.getDisplayName());
            message.setFirstName(userData.getFirstName());
            message.setLastName(userData.getLastName());

            // Essential contact info
            message.setEmail(userData.getEmail());
            message.setTitle(userData.getTitle());

            // Status
            message.setStatusText(userData.getStatusText());

            // Primary avatar (simplified - only keep the most commonly used sizes)
            message.setImageOriginal(userData.getImageOriginal());
            message.setImage48(userData.getImage48()); // Most common size for UI
            message.setImage192(userData.getImage192()); // Good for larger displays

        } catch (Exception e) {
            logger.warn("💥 User Data Copy Failed: Error copying user data for slackUserId: {} 😢",
                       userData.getSlackUserId(), e);
        }
    }

    /**
     * Calculate optimal overlap size using 80/20 strategy.
     * Each batch should contain 80% new messages + 20% old (processed) messages.
     *
     * Rules:
     * - If new messages >= 20: add 25% of new message count for processed messages
     * - If new messages < 5: add only 1 old message
     * - For other cases: calculate proportionally to maintain 80/20 balance
     *
     * @param effectiveBatchSize The effective batch size
     * @param unprocessedCount Number of unprocessed (new) messages available
     * @return Calculated overlap size
     */
    private int calculateOptimalOverlapSize(int effectiveBatchSize, long unprocessedCount) {
        // If no unprocessed messages, no overlap needed
        if (unprocessedCount == 0) {
            logger.debug("🔍 Overlap Calculation: No unprocessed messages - overlap size = 0");
            return 0;
        }

        // Apply 80/20 strategy rules
        int overlapSize;

        if (unprocessedCount >= 20) {
            // Rule: If new messages >= 20, add 25% of new message count for processed messages
            overlapSize = (int) (unprocessedCount * 0.25);
            logger.debug("📊 Overlap Strategy: {} new messages >= 20 → {} old messages (25% of new messages)",
                        unprocessedCount, overlapSize);
        } else if (unprocessedCount < 5) {
            // Rule: If new messages < 5, add only 1 old message
            overlapSize = 1;
            logger.debug("📊 Overlap Strategy: {} new messages < 5 → 1 old message (minimum overlap)", unprocessedCount);
        } else {
            // For 5-19 new messages, calculate proportionally
            // Target: maintain 80% new / 20% old ratio
            // old_messages = (new_messages * 20) / 80 = new_messages / 4
            overlapSize = Math.max(1, (int) (unprocessedCount / 4));
            logger.debug("📊 Overlap Strategy: {} new messages (5-19 range) → {} old messages (proportional 80/20)",
                        unprocessedCount, overlapSize);
        }

        // Safety check: don't exceed half of batch size
        int maxOverlap = effectiveBatchSize / 2;
        if (overlapSize > maxOverlap) {
            logger.warn("⚠️ Overlap Limit: Calculated overlap {} exceeds half batch size {}. Using {} instead.",
                       overlapSize, effectiveBatchSize, maxOverlap);
            overlapSize = maxOverlap;
        }

        logger.info("✅ Overlap Decision: {} new messages → {} old messages (80% new / 20% old strategy) 🎯",
                   unprocessedCount, overlapSize);

        return overlapSize;
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

                    // NOTE: Messages are no longer marked as processed immediately here.
                    // They will be marked as processed only after successful AI response
                    // in PostProcessingConsumer when AI returns successful results.

                    totalProcessed += currentBatch.size();

                    logger.info("✅ Batch {} Success: Sent {} messages for AI processing ({} were new, {} were already processed) 🤖",
                               batchNumber, currentBatch.size(), unprocessedInBatch, processedInBatch);

                    // Clean up this batch after successful sending for AI processing
                    // Keep overlap messages for next batch but don't mark as processed yet
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
     * This method is called after successful AI processing to ensure message lifecycle integrity.
     *
     * @param messages The list of messages to mark as processed
     */
    public void markMessagesAsProcessed(List<Message> messages) {
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
     * Mark messages as processed by their IDs after successful AI processing.
     * This method is called from PostProcessingConsumer when AI returns successful results.
     *
     * @param messageIds List of message IDs to mark as processed
     * @param tenantId Tenant ID for message lookup
     * @param deemergeUserId DeemergeUserId for message lookup
     */
    public void markMessagesAsProcessedByIds(List<String> messageIds, String tenantId, String deemergeUserId) {
        if (messageIds == null || messageIds.isEmpty()) {
            logger.debug("✅ No Message IDs to Mark: Empty message ID list provided (nothing to do here! 🎯)");
            return;
        }

        logger.info("📝 Processing Status Update: Marking {} messages as processed by IDs... | Tenant: {} | User: {}",
                   messageIds.size(), tenantId, deemergeUserId);

        try {
            // Load messages by their IDs from the specific tenant and user
            List<Message> allFoundMessages = new ArrayList<>();
            messageRepository.findAllById(messageIds).forEach(allFoundMessages::add);

            List<Message> messages = allFoundMessages.stream()
                .filter(message -> tenantId.equals(message.getTenantId()) && deemergeUserId.equals(message.getDeemergeUserId()))
                .collect(Collectors.toList());

            if (messages.isEmpty()) {
                logger.warn("⚠️ No Messages Found: Could not find any messages with provided IDs for tenant {} user {}",
                           tenantId, deemergeUserId);
                return;
            }

            logger.info("🔍 Found {} out of {} messages to mark as processed", messages.size(), messageIds.size());

            // Mark these messages as processed
            markMessagesAsProcessed(messages);

        } catch (Exception e) {
            logger.error("💥 Processing Update Failed: Error marking messages as processed by IDs: {} 😰", e.getMessage(), e);
            throw new RuntimeException("Failed to mark messages as processed by IDs", e);
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

    /**
     * Updates workspace processing status after successful processing.
     * This method persists the processing state to enable intelligent decisions
     * about when to process workspaces again.
     *
     * @param workspace The workspace that was processed
     * @param totalMessages Total number of messages in the workspace
     * @param processedMessages Number of messages processed in this run
     */
    private void updateWorkspaceProcessingStatus(Workspace workspace, int totalMessages, int processedMessages) {
        try {
            // Update processing timestamps and counts
            workspace.setLastProcessedAt(Instant.now());
            workspace.setLastProcessedMessageCount((long) totalMessages);
            workspace.setUnprocessedMessageCount((long) Math.max(0, totalMessages - processedMessages));

            // Save the updated workspace
            workspaceRepository.save(workspace);

            logger.info("📊 WORKSPACE-UPDATE: Updated processing status for workspace [{}] - " +
                       "totalMessages: {}, processedThisRun: {}, unprocessedRemaining: {}, lastProcessedAt: {} 🎯",
                       workspace.getId(), totalMessages, processedMessages, workspace.getUnprocessedMessageCount(),
                       workspace.getLastProcessedAt());

        } catch (Exception e) {
            logger.error("💥 WORKSPACE-UPDATE: Failed to update processing status for workspace [{}] - {} 😢",
                        workspace.getId(), e.getMessage(), e);
        }
    }
}
