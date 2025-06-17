package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.model.Message;
import com.lucid.automation.airouting.repository.MessageRepository;
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
    
    @Value("${sliding.window.default.batch.size:1000}")
    private int defaultBatchSize;
    
    @Value("${sliding.window.default.overlap.percentage:20}")
    private int defaultOverlapPercentage;
    
    @Value("${sliding.window.cleanup.enabled:true}")
    private boolean cleanupEnabled;
    
    public SlidingWindowService(MessageRepository messageRepository) {
        this.messageRepository = messageRepository;
    }
    
    /**
     * Process messages for a workspace using sliding window approach.
     * Messages are loaded from Redis, sorted chronologically, and processed in batches with overlap.
     * After processing, deletes all processed messages and keeps only the latest N messages (where N = overlap size).
     * 
     * @param workspaceId The workspace ID to process messages for
     * @param maxMessage Maximum number of messages to process per batch
     * @param overlaping Percentage of messages to keep as overlap between batches (0-100)
     * @param callback Function to process each batch of messages
     * @return Total number of messages processed
     */
    public int processMessages(String workspaceId, int maxMessage, int overlaping, 
                              Function<List<Message>, Void> callback) {
        
        if (workspaceId == null || workspaceId.trim().isEmpty()) {
            logger.warn("Cannot process messages: workspaceId is null or empty");
            return 0;
        }
        
        if (maxMessage <= 0) {
            logger.warn("Invalid maxNumberMessage: {}. Using default batch size: {}", 
                       maxMessage, defaultBatchSize);
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
            logger.info("Starting sliding window processing for workspaceId: {}, batchSize: {}, overlap: {}%", 
                       workspaceId, maxMessage, overlaping);
            
            // Load all messages for the workspace
            List<Message> allMessages = loadMessagesForWorkspace(workspaceId);
            
            if (allMessages.isEmpty()) {
                logger.info("No messages found for workspaceId: {}", workspaceId);
                return 0;
            }
            
            logger.info("Loaded {} messages for workspaceId: {}", allMessages.size(), workspaceId);
            
            // Sort messages chronologically by messageTs
            allMessages.sort(Comparator.comparing(Message::getMessageTs));
            
            // Calculate overlap size
            int overlapSize = (maxMessage * overlaping) / 100;
            
            logger.debug("Processing with batchSize: {}, overlapSize: {}", maxMessage, overlapSize);
            
            return processBatchesWithOverlap(allMessages, maxMessage, overlapSize, callback);
            
        } catch (Exception e) {
            logger.error("Error during sliding window processing for workspaceId: {}", workspaceId, e);
            return 0;
        }
    }
    
    /**
     * Process messages for a workspace using default configuration.
     * 
     * @param workspaceId The workspace ID to process messages for
     * @param callbackFunction Function to process each batch of messages
     * @return Total number of messages processed
     */
    public int processMessages(String workspaceId, Function<List<Message>, Void> callbackFunction) {
        return processMessages(workspaceId, defaultBatchSize, defaultOverlapPercentage, callbackFunction);
    }
    
    /**
     * Load all messages for a specific workspace from Redis.
     * 
     * @param workspaceId The workspace ID
     * @return List of messages sorted chronologically
     */
    private List<Message> loadMessagesForWorkspace(String workspaceId) {
        try {
            logger.debug("Loading messages for workspaceId: {}", workspaceId);
            
            List<Message> messages = messageRepository.findByWorkspaceId(workspaceId);
            
            logger.debug("Found {} messages for workspaceId: {}", messages.size(), workspaceId);
            
            return messages;
            
        } catch (Exception e) {
            logger.error("Error loading messages for workspaceId: {}", workspaceId, e);
            return Collections.emptyList();
        }
    }
    
    /**
     * Process messages in batches with sliding window overlap.
     * After processing all batches, deletes all processed messages from Redis and keeps only the latest N messages
     * where N = overlapSize.
     * 
     * @param allMessages All messages to process
     * @param batchSize Size of each batch
     * @param overlapSize Number of latest messages to keep after processing
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
        boolean allBatchesSuccessful = true;
        
        while (startIndex < allMessages.size()) {
            // Calculate end index for current batch
            int endIndex = Math.min(startIndex + batchSize, allMessages.size());
            
            // Extract current batch
            List<Message> currentBatch = new ArrayList<>(allMessages.subList(startIndex, endIndex));
            
            logger.debug("Processing batch {}: messages [{}-{}] (size: {})", 
                        batchNumber, startIndex, endIndex - 1, currentBatch.size());
            
            try {
                // Process the batch using the callback function
                callbackFunction.apply(currentBatch);
                
                totalProcessed += currentBatch.size();
                
                logger.debug("Successfully processed batch {} with {} messages", 
                           batchNumber, currentBatch.size());
                
            } catch (Exception e) {
                logger.error("Error processing batch {} for messages [{}-{}]", 
                           batchNumber, startIndex, endIndex - 1, e);
                allBatchesSuccessful = false;
                // Continue processing other batches even if one fails
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
        
        // After processing all batches successfully, clean up database if cleanup is enabled
        if (cleanupEnabled && allBatchesSuccessful && totalProcessed > 0) {
            cleanupProcessedMessages(allMessages, overlapSize);
        }
        
        logger.info("Completed sliding window processing: {} batches, {} total messages processed", 
                   batchNumber - 1, totalProcessed);
        
        return totalProcessed;
    }
    
    /**
     * Get statistics about messages for a workspace.
     * 
     * @param workspaceId The workspace ID
     * @return Statistics about the workspace messages
     */
    public WorkspaceMessageStats getWorkspaceStats(String workspaceId) {
        if (workspaceId == null || workspaceId.trim().isEmpty()) {
            return new WorkspaceMessageStats(workspaceId, 0, 0, 0);
        }
        
        try {
            List<Message> messages = loadMessagesForWorkspace(workspaceId);
            
            long totalMessages = messages.size();
            long uniqueChannels = messages.stream()
                    .map(Message::getChannelId)
                    .distinct()
                    .count();
            long uniqueUsers = messages.stream()
                    .map(Message::getUserId)
                    .filter(userId -> userId != null && !userId.trim().isEmpty())
                    .distinct()
                    .count();
            
            return new WorkspaceMessageStats(workspaceId, totalMessages, uniqueChannels, uniqueUsers);
            
        } catch (Exception e) {
            logger.error("Error getting workspace stats for workspaceId: {}", workspaceId, e);
            return new WorkspaceMessageStats(workspaceId, 0, 0, 0);
        }
    }
    
    /**
     * Process messages for a workspace with channel filtering.
     * 
     * @param workspaceId The workspace ID
     * @param channelIds List of channel IDs to filter by (null for all channels)
     * @param maxNumberMessage Maximum number of messages per batch
     * @param keepOverlaping Percentage of overlap between batches
     * @param callbackFunction Function to process each batch
     * @return Total number of messages processed
     */
    public int processMessagesForChannels(String workspaceId, List<String> channelIds, 
                                         int maxNumberMessage, int keepOverlaping, 
                                         Function<List<Message>, Void> callbackFunction) {
        
        if (workspaceId == null || workspaceId.trim().isEmpty()) {
            logger.warn("Cannot process messages: workspaceId is null or empty");
            return 0;
        }
        
        try {
            List<Message> allMessages = loadMessagesForWorkspace(workspaceId);
            
            // Filter by channels if specified
            if (channelIds != null && !channelIds.isEmpty()) {
                allMessages = allMessages.stream()
                        .filter(message -> channelIds.contains(message.getChannelId()))
                        .collect(Collectors.toList());
                
                logger.info("Filtered messages to {} messages for channels: {}", 
                           allMessages.size(), channelIds);
            }
            
            if (allMessages.isEmpty()) {
                logger.info("No messages found for workspaceId: {} and channels: {}", 
                           workspaceId, channelIds);
                return 0;
            }
            
            // Sort messages chronologically
            allMessages.sort(Comparator.comparing(Message::getMessageTs));
            
            // Calculate overlap size
            int overlapSize = (maxNumberMessage * keepOverlaping) / 100;
            
            return processBatchesWithOverlap(allMessages, maxNumberMessage, overlapSize, callbackFunction);
            
        } catch (Exception e) {
            logger.error("Error processing messages for workspaceId: {} and channels: {}", 
                        workspaceId, channelIds, e);
            return 0;
        }
    }
    
    /**
     * Clean up processed messages from Redis database, keeping only the latest N messages.
     * This method deletes all processed messages except for the most recent ones based on overlapSize.
     * 
     * @param allMessages All messages that were processed
     * @param overlapSize Number of latest messages to keep in Redis
     */
    private void cleanupProcessedMessages(List<Message> allMessages, int overlapSize) {
        if (allMessages == null || allMessages.isEmpty()) {
            logger.debug("No messages to cleanup");
            return;
        }
        
        try {
            // Sort messages by timestamp (newest first) to identify latest messages to keep
            List<Message> sortedMessages = allMessages.stream()
                    .sorted(Comparator.comparing(Message::getMessageTs).reversed())
                    .collect(Collectors.toList());
            
            // Determine how many messages to delete
            int messagesToKeep = Math.min(overlapSize, sortedMessages.size());
            int messagesToDelete = sortedMessages.size() - messagesToKeep;
            
            if (messagesToDelete <= 0) {
                logger.info("No messages to delete. Total: {}, Keeping: {}", 
                           sortedMessages.size(), messagesToKeep);
                return;
            }
            
            // Get the messages to delete (all except the latest N)
            List<Message> messagesForDeletion = sortedMessages.subList(messagesToKeep, sortedMessages.size());
            
            // Extract IDs for batch deletion
            List<String> messageIds = messagesForDeletion.stream()
                    .map(Message::getId)
                    .collect(Collectors.toList());
            
            // Delete messages from Redis
            messageRepository.deleteAllById(messageIds);
            
            logger.info("Successfully cleaned up {} processed messages, keeping {} latest messages in Redis", 
                       messagesToDelete, messagesToKeep);
            
            if (logger.isDebugEnabled()) {
                logger.debug("Deleted {} message IDs, kept {} latest messages", 
                           messageIds.size(), messagesToKeep);
            }
            
        } catch (Exception e) {
            logger.error("Error cleaning up processed messages from Redis: {}", e.getMessage(), e);
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
    public int cleanupWorkspace(String workspaceId, int keepRecentCount) {
        if (workspaceId == null || workspaceId.trim().isEmpty()) {
            logger.warn("Cannot cleanup workspace: workspaceId is null or empty");
            return 0;
        }
        
        if (keepRecentCount < 0) {
            logger.warn("Invalid keepRecentCount: {}. Using 0", keepRecentCount);
            keepRecentCount = 0;
        }
        
        try {
            logger.info("Starting cleanup for workspaceId: {}, keeping {} recent messages", 
                       workspaceId, keepRecentCount);
            
            // Load all messages for the workspace
            List<Message> allMessages = loadMessagesForWorkspace(workspaceId);
            
            if (allMessages.isEmpty()) {
                logger.info("No messages found for cleanup in workspaceId: {}", workspaceId);
                return 0;
            }
            
            // Sort messages chronologically by messageTs (newest first for keeping recent)
            allMessages.sort(Comparator.comparing(Message::getMessageTs).reversed());
            
            if (allMessages.size() <= keepRecentCount) {
                logger.info("Workspace {} has {} messages, keeping all (requested to keep {})", 
                           workspaceId, allMessages.size(), keepRecentCount);
                return 0;
            }
            
            // Determine which messages to delete
            List<Message> messagesToDelete = allMessages.subList(keepRecentCount, allMessages.size());
            
            // Extract IDs for batch deletion
            List<String> messageIds = messagesToDelete.stream()
                    .map(Message::getId)
                    .collect(Collectors.toList());
            
            // Delete messages from Redis
            messageRepository.deleteAllById(messageIds);
            
            logger.info("Successfully cleaned up workspace {}: deleted {} messages, kept {} recent messages", 
                       workspaceId, messagesToDelete.size(), keepRecentCount);
            
            return messagesToDelete.size();
            
        } catch (Exception e) {
            logger.error("Error during workspace cleanup for workspaceId: {}", workspaceId, e);
            return 0;
        }
    }
    
    /**
     * Get count of messages that would be deleted in a cleanup operation.
     * Useful for preview before actual cleanup.
     * 
     * @param workspaceId The workspace ID to analyze
     * @param keepRecentCount Number of most recent messages to keep
     * @return Number of messages that would be deleted
     */
    public int previewCleanup(String workspaceId, int keepRecentCount) {
        if (workspaceId == null || workspaceId.trim().isEmpty()) {
            return 0;
        }
        
        try {
            List<Message> allMessages = loadMessagesForWorkspace(workspaceId);
            
            if (allMessages.size() <= keepRecentCount) {
                return 0;
            }
            
            return allMessages.size() - keepRecentCount;
            
        } catch (Exception e) {
            logger.error("Error previewing cleanup for workspaceId: {}", workspaceId, e);
            return 0;
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
