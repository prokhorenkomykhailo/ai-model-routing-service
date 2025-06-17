package com.lucid.automation.airouting.scheduler;

import com.lucid.automation.airouting.model.AITaskType;
import com.lucid.automation.airouting.model.Message;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.SlackParticipant;
import com.lucid.automation.airouting.model.Workspace;
import com.lucid.automation.airouting.service.AIMessagePublisherService;
import com.lucid.automation.airouting.service.SlidingWindowService;
import com.lucid.automation.airouting.service.WorkspaceService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Scheduler for enriching messages from data storage service.
 * This scheduler fetches all group IDs, loads messages for each group,
 * and publishes them to the ai.enrich.queue for AI processing.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(value = "ai.enrichment.scheduler.enabled", havingValue = "true", matchIfMissing = true)
public class MessageEnrichmentScheduler {

    private final AIMessagePublisherService aiMessagePublisherService;
    private final WorkspaceService workspaceService;
    private final SlidingWindowService slidingWindowService;

    @Value("${ai.enrichment.scheduler.batch-size:50}")
    private int batchSize;

    @Value("${ai.enrichment.scheduler.max-retries:3}")
    private int maxRetries;

    @Value("${ai.enrichment.scheduler.default-tenant-id:default}")
    private String defaultTenantId;

    @Value("${ai.enrichment.scheduler.default-tenant-schema:public}")
    private String defaultTenantSchema;

    /**
     * Scheduled method that runs based on the cron expression in application.yml.
     * Gets all workspaces from Redis and processes messages for each workspace.
     */
    @Scheduled(cron = "${ai.enrichment.scheduler.cron:0 */5 * * * ?}")
    public void processMessageEnrichment() {
        log.info("Starting scheduled message enrichment process");
        
        try {
            // Step 1: Get all workspaces from Redis
            List<Workspace> workspaces = workspaceService.getAllWorkspaces();
            log.info("Found {} workspaces to process for enrichment", workspaces.size());

            if (workspaces.isEmpty()) {
                log.info("No workspaces found, skipping enrichment cycle");
                return;
            }

            int processedWorkspaces = 0;
            int totalBatches = 0;
            int totalMessages = 0;

            // Step 2: Process each workspace using SlidingWindowService
            for (Workspace workspace : workspaces) {
                try {
                    log.info("Processing workspace: {} (ID: {}) - tenant: {} schema: {}", 
                            workspace.getName(), workspace.getId(), workspace.getTenantId(), workspace.getTenantSchema());
                    
                    int[] results = processWorkspace(workspace);
                    int workspaceBatches = results[0];
                    int workspaceMessages = results[1];
                    
                    totalBatches += workspaceBatches;
                    totalMessages += workspaceMessages;
                    processedWorkspaces++;
                    
                    log.debug("Processed {} batches with {} messages for workspace: {}", 
                             workspaceBatches, workspaceMessages, workspace.getName());
                } catch (Exception e) {
                    log.error("Failed to process workspace: {} ({}). Error: {}", 
                             workspace.getName(), workspace.getId(), e.getMessage(), e);
                    // Continue processing other workspaces even if one fails
                }
            }

            log.info("Completed scheduled message enrichment process using SlidingWindowService. " +
                    "Processed {} workspaces with {} total batches and {} total messages", 
                    processedWorkspaces, totalBatches, totalMessages);

        } catch (Exception e) {
            log.error("Error during scheduled message enrichment process: {}", e.getMessage(), e);
        }
    }

    /**
     * Processes messages for a specific workspace using SlidingWindowService.
     * Uses sliding window approach to process messages from Redis in batches.
     * 
     * @param workspace the workspace to process
     * @return array with [totalBatches, totalMessages] processed
     */
    private int[] processWorkspace(Workspace workspace) {
        try {
            String workspaceId = workspace.getId();
            
            log.info("Processing workspace: {} using SlidingWindowService", workspace.getName());
            
            // Get workspace statistics first
            SlidingWindowService.WorkspaceMessageStats stats = 
                    slidingWindowService.getWorkspaceStats(workspaceId);
            
            log.info("Workspace {} stats: {}", workspace.getName(), stats);
            
            if (stats.getTotalMessages() == 0) {
                log.info("No messages found in Redis for workspace: {}", workspace.getName());
                return new int[]{0, 0};
            }
            
            // Track processing metrics
            AtomicInteger batchCount = new AtomicInteger(0);
            AtomicInteger totalProcessed = new AtomicInteger(0);
            
            // Define the processing callback for each batch
            Function<List<Message>, Void> enrichmentProcessor = 
                messages -> {
                    int currentBatch = batchCount.incrementAndGet();
                    log.info("Processing batch #{} with {} messages for workspace: {}", 
                           currentBatch, messages.size(), workspace.getName());
                    
                    try {
                        // Process messages for AI enrichment
                        processMessageBatchForEnrichment(messages, workspace, currentBatch);
                        totalProcessed.addAndGet(messages.size());
                        
                        log.debug("Successfully processed batch #{} for workspace: {}", 
                                currentBatch, workspace.getName());
                        
                    } catch (Exception e) {
                        log.error("Error processing batch #{} for workspace {}: {}", 
                                currentBatch, workspace.getName(), e.getMessage(), e);
                        // Don't throw exception here - let other batches continue
                    }
                    
                    return null;
                };
            
            // Process messages using sliding window
            int messagesProcessed = slidingWindowService.processMessages(
                    workspaceId, batchSize, 20, enrichmentProcessor);
            
            log.info("Completed processing workspace: {} - {} batches, {} messages processed", 
                    workspace.getName(), batchCount.get(), messagesProcessed);
            
            return new int[]{batchCount.get(), messagesProcessed};

        } catch (Exception e) {
            log.error("Error processing workspace: {}. Error: {}", workspace.getName(), e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Process a batch of messages from Redis for AI enrichment.
     * Converts Redis Message objects to the format expected by the AI service.
     * Processes all messages in the batch as a single unit without grouping.
     * 
     * @param messages The batch of messages from Redis
     * @param workspace The workspace context
     * @param batchNumber The current batch number for logging
     */
    private void processMessageBatchForEnrichment(List<Message> messages, Workspace workspace, int batchNumber) {
        if (messages == null || messages.isEmpty()) {
            log.debug("No messages to process in batch #{} for workspace: {}", batchNumber, workspace.getName());
            return;
        }
        
        try {
            log.debug("Processing batch #{} with {} messages for workspace: {}", 
                     batchNumber, messages.size(), workspace.getName());
            
            // Convert all Redis Message objects to SlackMessage format
            List<SlackMessage> slackMessages = new ArrayList<>();
            List<SlackParticipant> participants = new ArrayList<>();
            
            // Process all messages in the batch
            for (Message message : messages) {
                SlackMessage slackMessage = new SlackMessage();
                slackMessage.setId(message.getId());
                slackMessage.setTs(message.getMessageTs());
                slackMessage.setUserId(message.getUserId());
                slackMessage.setUsername(message.getUsername());
                slackMessage.setText(message.getText());
                slackMessage.setChannelId(message.getChannelId());
                slackMessage.setThreadTs(message.getThreadTs());
                slackMessage.setType(message.getMessageType());
                slackMessage.setSubtype(message.getSubtype());
                
                // Convert timestamp from String to LocalDateTime if needed
                if (message.getMessageTs() != null) {
                    try {
                        // Assuming messageTs is in epoch seconds format
                        long epochSeconds = Long.parseLong(message.getMessageTs().split("\\.")[0]);
                        slackMessage.setTimestamp(LocalDateTime.ofEpochSecond(
                            epochSeconds, 0, ZoneOffset.UTC));
                    } catch (Exception e) {
                        log.warn("Could not parse timestamp for message {}: {}", message.getId(), e.getMessage());
                    }
                }
                
                slackMessages.add(slackMessage);
                
                // Add participant if not already present
                if (message.getUserId() != null && message.getUsername() != null) {
                    boolean participantExists = participants.stream()
                        .anyMatch(p -> message.getUserId().equals(p.getId()));
                    
                    if (!participantExists) {
                        SlackParticipant participant = new SlackParticipant();
                        participant.setId(message.getUserId());
                        participant.setUsername(message.getUsername());
                        participants.add(participant);
                    }
                }
            }
            
            // Publish AI enrichment request for all messages in the batch
            if (!slackMessages.isEmpty()) {
                String conversationId = workspace.getId() + ":batch_" + batchNumber;
                
                // Create context map with batch information
                Map<String, Object> context = new HashMap<>();
                context.put("workspaceId", workspace.getId());
                context.put("workspaceName", workspace.getName());
                context.put("batchNumber", batchNumber);
                context.put("messageCount", slackMessages.size());
                context.put("participantCount", participants.size());
                
                // Publish to AI enrichment queue using the existing method
                String messageId = aiMessagePublisherService.publishAIRequest(
                    AITaskType.ENRICH_CONVERSATION,
                    "", // content - empty for conversation enrichment
                    workspace.getTenantId(),
                    workspace.getTenantSchema() != null ? workspace.getTenantSchema() : defaultTenantSchema,
                    null, // userId - not needed for conversation enrichment
                    conversationId,
                    slackMessages,
                    participants,
                    context,
                    null, // preferredProvider
                    null  // replyTopic
                );
                
                if (messageId != null) {
                    log.debug("Successfully published batch #{} with {} messages to AI enrichment queue with messageId: {}", 
                            batchNumber, slackMessages.size(), messageId);
                } else {
                    log.warn("Failed to publish batch #{} with {} messages to AI enrichment queue", 
                           batchNumber, slackMessages.size());
                }
            }
            
            log.info("Completed processing batch #{} with {} messages for workspace: {}", 
                    batchNumber, messages.size(), workspace.getName());
            
        } catch (Exception e) {
            log.error("Error processing message batch #{} for workspace {}: {}", 
                     batchNumber, workspace.getName(), e.getMessage(), e);
            throw e;
        }
    }
}
