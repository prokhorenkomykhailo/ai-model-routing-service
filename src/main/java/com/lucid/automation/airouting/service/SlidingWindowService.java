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
     * Messages are loaded from Redis, sorted chronologically, and processed in batches with overlap.
     * After processing, always deletes all processed messages and keeps only the latest N messages (where N = overlap size).
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
            List<Message> allMessages = loadMessagesForWorkspace(deemergeUserId);
            
            if (allMessages.isEmpty()) {
                logger.info("No messages found for workspaceId: {}", workspace);
                return 0;
            }
            
            logger.info("Loaded {} messages for workspaceId: {}", allMessages.size(), workspace);
            
            // Sort messages chronologically by messageTs Ascending
            // This ensures we process messages in the order they were sent from oldest to newest
            logger.debug("Sorting messages chronologically by messageTs");
            allMessages.sort(Comparator.comparing(Message::getMessageTs));
            
            
            // Calculate overlap size, minimum of 10 messages or 20% of maxMessage
            int overlapSize = (maxMessage * overlaping) / 100;
            overlapSize = Math.max(overlapSize, 10);
            
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
     * @param workspaceId The workspace ID
     * @return List of messages sorted chronologically
     */
    private List<Message> loadMessagesForWorkspace(String deemergeUserId) {
        try {
            logger.debug("Loading messages for workspaceId: {}", deemergeUserId);

            List<Message> messages = messageRepository.findAllByDeemergeUserId(deemergeUserId);
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
            
            logger.debug("Found {}", messages.size());
            
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
            
            logger.debug("Successfully updated message {} with user data for slackUserId: {}", 
                        message.getId(), userData.getSlackUserId());
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
            
            logger.debug("Processing batch {}: messages [{}-{}] (size: {})", 
                        batchNumber, startIndex, endIndex - 1, currentBatch.size());
            
            try {
                // Process the batch using the callback function
                callbackFunction.apply(currentBatch);
                
                totalProcessed += currentBatch.size();
                
                logger.debug("Successfully processed batch {} with {} messages", 
                           batchNumber, currentBatch.size());
                
                // Immediately clean up this batch after successful processing
                cleanupBatchMessages(currentBatch, overlapSize, batchNumber);
                
            } catch (Exception e) {
                logger.error("Error processing batch {} for messages [{}-{}]", 
                           batchNumber, startIndex, endIndex - 1, e);
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
        
        logger.info("Completed sliding window processing: {} batches, {} total messages processed", 
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
        
        try {
            // Sort messages by timestamp (newest first) to identify latest messages to keep for overlap
            List<Message> sortedMessages = batchMessages.stream()
                    .sorted(Comparator.comparing(Message::getMessageTs).reversed())
                    .collect(Collectors.toList());
            
            // Determine how many messages to delete from this batch
            int messagesToKeep = Math.min(overlapSize, sortedMessages.size());
            int messagesToDelete = sortedMessages.size() - messagesToKeep;
            
            if (messagesToDelete <= 0) {
                logger.debug("Batch {}: No messages to delete. Total: {}, Keeping: {} for overlap", 
                           batchNumber, sortedMessages.size(), messagesToKeep);
                return;
            }
            
            // Get the messages to delete (all except the latest N for overlap)
            List<Message> messagesForDeletion = sortedMessages.subList(messagesToKeep, sortedMessages.size());
            
            // Extract IDs for batch deletion
            List<String> messageIds = messagesForDeletion.stream()
                    .map(Message::getId)
                    .collect(Collectors.toList());
            
            // Delete messages from Redis
            messageRepository.deleteAllById(messageIds);
            
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
