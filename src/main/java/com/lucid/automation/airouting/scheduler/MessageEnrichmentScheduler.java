package com.lucid.automation.airouting.scheduler;

import com.lucid.automation.airouting.model.AITaskType;
import com.lucid.automation.airouting.model.Message;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.SlackParticipant;
import com.lucid.automation.airouting.model.Workspace;
import com.lucid.automation.airouting.producer.AIMessageProducer;
import com.lucid.automation.airouting.service.SlidingWindowService;
import com.lucid.automation.airouting.service.WorkspaceService;
import com.lucid.automation.airouting.util.TimestampUtil;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Scheduler for enriching messages from data storage service.
 * Fetches workspace information, loads messages for each workspace,
 * and publishes them to the AI enrichment queue using a sliding window approach.
 *
 * Enhanced with protection against processing old/stale messages:
 * - Filters messages older than configurable threshold (default: 30 days)
 * - Skips workspaces with insufficient recent messages (default: minimum 1)
 * - Comprehensive logging for debugging message processing flow
 * - Integration with SlidingWindowService for intelligent processing decisions
 */
@Slf4j
@Component
@ConditionalOnProperty(value = "ai.enrichment.scheduler.enabled", havingValue = "true", matchIfMissing = false)
public final class MessageEnrichmentScheduler implements InitializingBean {

    // Configuration constants
    private static final int DEFAULT_BATCH_SIZE = 1000;
    private static final int DEFAULT_SLIDING_WINDOW_SIZE = 20;
    private static final String DEFAULT_TENANT_SCHEMA = "public";
    private static final String BATCH_CONVERSATION_ID_SEPARATOR = ":batch_";

    // Message processing thresholds to prevent processing old messages
    private static final int DEFAULT_MAX_MESSAGE_AGE_DAYS = 90; // Don't process messages older than 90 days
    private static final int DEFAULT_MIN_RECENT_MESSAGES = 1;   // Skip processing if no recent messages

    // Add scheduler-level counters
    private final AtomicInteger totalSchedulerRuns = new AtomicInteger(0);
    private final AtomicInteger totalBatchesProcessed = new AtomicInteger(0);
    private final AtomicInteger totalMessagesEnriched = new AtomicInteger(0);
    private final AtomicInteger successfulWorkspaces = new AtomicInteger(0);
    private final AtomicInteger failedWorkspaces = new AtomicInteger(0);

    // Dependencies
    private final AIMessageProducer aiMessageProducer;
    private final WorkspaceService workspaceService;
    private final SlidingWindowService slidingWindowService;
    private final ApplicationContext applicationContext;
    private final int batchSize;
    private final String defaultTenantSchema;
    private final int maxMessageAgeDays;
    private final int minRecentMessages;

    /**
     * Constructor with dependency and configuration injection.
     */
    public MessageEnrichmentScheduler(
            final AIMessageProducer aiMessageProducer,
            final WorkspaceService workspaceService,
            final SlidingWindowService slidingWindowService,
            final ApplicationContext applicationContext,
            @Value("${ai.enrichment.scheduler.batch-size:" + DEFAULT_BATCH_SIZE + "}") final int batchSize,
            @Value("${ai.enrichment.scheduler.default-tenant-schema:" + DEFAULT_TENANT_SCHEMA + "}") final String defaultTenantSchema,
            @Value("${ai.enrichment.scheduler.max-message-age-days:" + DEFAULT_MAX_MESSAGE_AGE_DAYS + "}") final int maxMessageAgeDays,
            @Value("${ai.enrichment.scheduler.min-recent-messages:" + DEFAULT_MIN_RECENT_MESSAGES + "}") final int minRecentMessages) {

        this.aiMessageProducer = aiMessageProducer;
        this.workspaceService = workspaceService;
        this.slidingWindowService = slidingWindowService;
        this.applicationContext = applicationContext;
        this.batchSize = batchSize;
        this.defaultTenantSchema = defaultTenantSchema;
        this.maxMessageAgeDays = maxMessageAgeDays;
        this.minRecentMessages = minRecentMessages;
    }

    @PostConstruct
    public void postConstruct() {
        log.info("📅 === MessageEnrichmentScheduler @PostConstruct Called ===");
        log.info("⚙️ [CONFIG] Message Processing Configuration:");
        log.info("⚙️ [CONFIG]   - Batch Size: {}", batchSize);
        log.info("⚙️ [CONFIG]   - Max Message Age: {} days", maxMessageAgeDays);
        log.info("⚙️ [CONFIG]   - Min Recent Messages: {}", minRecentMessages);
        log.info("⚙️ [CONFIG]   - Default Tenant Schema: {}", defaultTenantSchema);

        // Check if scheduling is enabled globally
        try {
            String[] schedulingBeans = applicationContext.getBeanNamesForAnnotation(org.springframework.scheduling.annotation.EnableScheduling.class);
            log.info("📋 @EnableScheduling beans found: {}", java.util.Arrays.toString(schedulingBeans));
        } catch (Exception e) {
            log.warn("⚠️ Error checking @EnableScheduling beans: {}", e.getMessage());
        }

        // Check property values
        try {
            org.springframework.core.env.Environment env = applicationContext.getEnvironment();
            log.debug("🌍 Environment configured with {} active profiles", env.getActiveProfiles().length);
        } catch (Exception e) {
            log.warn("⚠️ Error checking environment properties: {}", e.getMessage());
        }

        // Check if this bean is being created
        log.info("✅ MessageEnrichmentScheduler bean successfully created and initialized");
        log.info("⚠️ [OLD-MESSAGE-PROTECTION] Scheduler will skip messages older than {} days and workspaces with fewer than {} recent messages",
            maxMessageAgeDays, minRecentMessages);
        log.info("📅 === MessageEnrichmentScheduler @PostConstruct Completed ===");
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("📅 === MessageEnrichmentScheduler afterPropertiesSet Called ===");
        log.info("✅ All dependencies have been injected successfully");

        // Schedule a test run in 10 seconds to verify scheduling works
        log.info("⏰ Scheduler will attempt to run every 15 minutes according to cron expression");
        log.info("📍 Next scheduled execution should occur at the next 15-minute interval");
        log.info("📅 === MessageEnrichmentScheduler afterPropertiesSet Completed ===");
    }

    /**
     * Scheduled method to process message enrichment for all workspaces.
     */
    @Scheduled(cron = "${ai.enrichment.scheduler.cron:0 */15 * * * ?}")
    public void processMessageEnrichment() {
        long schedulerStartTime = System.currentTimeMillis();
        totalSchedulerRuns.incrementAndGet();

        log.info("🚀 === SCHEDULER EXECUTION #{} STARTED === ⏰ {}",
            totalSchedulerRuns.get(), LocalDateTime.now());
        log.info("📊 [SCHEDULER-STATS] Historical totals - Runs: {} | Batches: {} | Messages: {} | ✅ Workspaces: {} | ❌ Failed: {}",
            totalSchedulerRuns.get(), totalBatchesProcessed.get(), totalMessagesEnriched.get(),
            successfulWorkspaces.get(), failedWorkspaces.get());

        try {
            final var workspaces = workspaceService.getAllWorkspaces();

            if (workspaces.isEmpty()) {
                log.info("📭 No workspaces found, skipping enrichment cycle");
                return;
            }

            // NEW: Global message availability check (SCRUM-393)
            log.info("🔍 [PRE-CHECK] Verifying message availability across {} workspaces before enrichment cycle #{}",
                workspaces.size(), totalSchedulerRuns.get());

            long totalUnprocessedMessages = calculateTotalUnprocessedMessages(workspaces);

            if (totalUnprocessedMessages == 0) {
                long checkTime = System.currentTimeMillis() - schedulerStartTime;
                log.info("⏭️ [SKIP-CYCLE] No unprocessed messages found across {} workspaces - skipping enrichment cycle #{} (check completed in {}ms)",
                    workspaces.size(), totalSchedulerRuns.get(), checkTime);
                log.info("🏁 === SCHEDULER EXECUTION #{} COMPLETED (SKIPPED) ===", totalSchedulerRuns.get());
                return; // Early exit without processing
            }

            log.info("✅ [PRE-CHECK] Found {} unprocessed messages across {} workspaces - proceeding with enrichment cycle #{}",
                totalUnprocessedMessages, workspaces.size(), totalSchedulerRuns.get());

            log.info("🏢 Processing {} workspaces for message enrichment", workspaces.size());
            processAllWorkspaces(workspaces);

            long totalTime = System.currentTimeMillis() - schedulerStartTime;
            log.info("✅ [SCHEDULER-COMPLETE] Execution #{} completed in {}ms for {} workspaces",
                totalSchedulerRuns.get(), totalTime, workspaces.size());

        } catch (Exception e) {
            long totalTime = System.currentTimeMillis() - schedulerStartTime;
            log.error("🚨 [SCHEDULER-ERROR] Critical error during execution #{} after {}ms: {}",
                totalSchedulerRuns.get(), totalTime, e.getMessage(), e);
        }

        log.info("🏁 === SCHEDULER EXECUTION #{} COMPLETED ===", totalSchedulerRuns.get());
    }

    /**
     * Calculate total unprocessed messages across all workspaces.
     * This is used for the pre-check validation (SCRUM-393) to determine if enrichment cycle should run.
     * Uses SlidingWindowService's shouldProcessWorkspace to determine if any workspace has messages.
     *
     * @param workspaces List of workspaces to check
     * @return Total count of unprocessed messages across all workspaces (>0 if any unprocessed messages exist)
     */
    private long calculateTotalUnprocessedMessages(final List<Workspace> workspaces) {
        // Check if ANY workspace has unprocessed messages that should be processed
        // This leverages the existing SlidingWindowService.shouldProcessWorkspace logic
        // which already checks for unprocessed message counts, time thresholds, etc.
        return workspaces.stream()
            .filter(workspace -> {
                try {
                    // SlidingWindowService.shouldProcessWorkspace already checks:
                    // - Invalid tenant ID (skips)
                    // - Unprocessed message count (must be > 0)
                    // - Time thresholds (must meet criteria)
                    SlidingWindowService.ProcessingDecision decision = slidingWindowService.shouldProcessWorkspace(workspace);
                    if (decision.shouldProcess()) {
                        log.debug("🔍 [PRE-CHECK] Workspace {} should be processed - has unprocessed messages",
                            workspace.getName());
                        return true;
                    }
                    return false;
                } catch (Exception e) {
                    log.warn("⚠️ [PRE-CHECK] Error checking workspace {}: {}",
                        workspace.getName(), e.getMessage());
                    return false; // Treat errors as no messages
                }
            })
            .count(); // Count of workspaces that have unprocessed messages
    }

    /**
     * Processes all workspaces for message enrichment with individual error handling.
     */
    private void processAllWorkspaces(final List<Workspace> workspaces) {
        int processedWorkspaces = 0;
        int skippedWorkspaces = 0;

        for (final var workspace : workspaces) {
            long workspaceStartTime = System.currentTimeMillis();
            try {
                // Let SlidingWindowService decide if workspace should be processed
                // This already checks for unprocessed messages, time thresholds, etc.
                final var results = processWorkspace(workspace);
                final int workspaceBatches = results[0];
                final int workspaceMessages = results[1];

                if (workspaceBatches > 0 || workspaceMessages > 0) {
                    totalBatchesProcessed.addAndGet(workspaceBatches);
                    totalMessagesEnriched.addAndGet(workspaceMessages);
                    successfulWorkspaces.incrementAndGet();
                    processedWorkspaces++;

                    long workspaceTime = System.currentTimeMillis() - workspaceStartTime;

                    log.info("✅ [WORKSPACE-SUCCESS] {} processed in {}ms | 📦 {} batches, 📨 {} messages | 🏢 Tenant: {} | 👤 User: {} | 📊 GLOBAL TOTALS - Batches: {} | Messages: {}",
                        workspace.getName(), workspaceTime, workspaceBatches, workspaceMessages,
                        workspace.getTenantId(), workspace.getDeemergeUserName(),
                        totalBatchesProcessed.get(), totalMessagesEnriched.get());
                } else {
                    skippedWorkspaces++;
                    long workspaceTime = System.currentTimeMillis() - workspaceStartTime;
                    log.info("⏭️ [WORKSPACE-SKIPPED] {} skipped in {}ms | No new messages or doesn't meet processing criteria | 🏢 Tenant: {} | 👤 User: {}",
                        workspace.getName(), workspaceTime, workspace.getTenantId(), workspace.getDeemergeUserName());
                }

            } catch (Exception e) {
                failedWorkspaces.incrementAndGet();
                long workspaceTime = System.currentTimeMillis() - workspaceStartTime;
                log.error("❌ [WORKSPACE-FAILED] {} failed after {}ms: {} | 📊 GLOBAL TOTALS - Success: {} | Failed: {}",
                    workspace.getName(), workspaceTime, e.getMessage(),
                    successfulWorkspaces.get(), failedWorkspaces.get(), e);
            }
        }

        log.info("📊 [SCHEDULER-SUMMARY] Session completed: ✅ {} processed, ⏭️ {} skipped, ❌ {} failed out of 📊 {} total workspaces",
            processedWorkspaces, skippedWorkspaces, failedWorkspaces.get() % workspaces.size(), workspaces.size());
    }

    /**
     * Processes messages for a specific workspace using SlidingWindowService.
     */
    private int[] processWorkspace(final Workspace workspace) {
        log.info("🔄 Starting message processing for workspace: {} | 🏢 Tenant: {} | 👤 User: {} using SlidingWindowService",
            workspace.getName(), workspace.getTenantId(), workspace.getDeemergeUserName());

        final var batchCount = new AtomicInteger(0);
        final var totalProcessed = new AtomicInteger(0);
        final Function<List<Message>, Void> enrichmentProcessor = createEnrichmentProcessor(workspace, batchCount, totalProcessed);

        try {
            final int messagesProcessed = slidingWindowService.processMessages(
                workspace, batchSize, DEFAULT_SLIDING_WINDOW_SIZE, enrichmentProcessor);

            log.info("✅ Completed processing workspace: {} | 📦 {} batches, 📨 {} messages processed, 🏢 Tenant: {}, 👤 User: {}",
                workspace.getName(), batchCount.get(), messagesProcessed, workspace.getTenantId(), workspace.getDeemergeUserName());

            return new int[]{batchCount.get(), messagesProcessed};

        } catch (Exception e) {
            log.error("🚨 Critical error processing workspace: {}. Error: {}", workspace.getName(), e.getMessage(), e);
            throw new RuntimeException("Failed to process workspace: " + workspace.getName(), e);
        }
    }

    /**
     * Creates the enrichment processor function for handling message batches.
     */
    private Function<List<Message>, Void> createEnrichmentProcessor(final Workspace workspace,
                                                                   final AtomicInteger batchCount,
                                                                   final AtomicInteger totalProcessed) {
        return messages -> {
            final int currentBatch = batchCount.incrementAndGet();
            log.info("📦 Processing batch #{} with 📨 {} messages for workspace: {} | 🏢 Tenant: {}",
                currentBatch, messages.size(), workspace.getName(), workspace.getTenantId());
            try {
                processMessageBatchForEnrichment(messages, workspace, currentBatch);
                totalProcessed.addAndGet(messages.size());
                log.debug("✅ Successfully processed batch #{} for workspace: {}", currentBatch, workspace.getName());
            } catch (Exception e) {
                log.error("❌ Error processing batch #{} for workspace {}: {}",
                    currentBatch, workspace.getName(), e.getMessage(), e);
            }
            return null;
        };
    }

    /**
     * Process a batch of messages from Redis for AI enrichment.
     * Converts Redis Message objects to SlackMessage format and processes them as a batch.
     * Includes filtering to prevent processing of old/stale messages.
     */
    private void processMessageBatchForEnrichment(final List<Message> messages,
                                                 final Workspace workspace,
                                                 final int batchNumber) {
        if (messages == null || messages.isEmpty()) {
            log.debug("📭 No messages to process in batch #{} for workspace: {}", batchNumber, workspace.getName());
            return;
        }

        long batchStartTime = System.currentTimeMillis();
        log.info("📦 [BATCH-START] Processing batch #{} with {} Redis messages for workspace: {} | 🏢 Tenant: {}",
            batchNumber, messages.size(), workspace.getName(), workspace.getTenantId());

        try {
            // Filter out old messages before processing
            final List<Message> recentMessages = filterRecentMessages(messages, workspace.getName(), batchNumber);

            if (recentMessages.isEmpty()) {
                log.warn("⚠️ [BATCH-FILTERED] Batch #{} for workspace {} has no recent messages after filtering - skipping",
                    batchNumber, workspace.getName());
                return;
            }

            final var slackMessages = new ArrayList<SlackMessage>();
            final var participants = new ArrayList<SlackParticipant>();

            processMessagesInBatch(recentMessages, slackMessages, participants);

            if (!slackMessages.isEmpty()) {
                // Calculate unique users and channels for statistics
                long uniqueUsers = slackMessages.stream().map(SlackMessage::getUniqueUserId).distinct().count();
                long uniqueChannels = slackMessages.stream().map(SlackMessage::getChannelId).distinct().count();

                long batchProcessingTime = System.currentTimeMillis() - batchStartTime;
                int filteredCount = messages.size() - recentMessages.size();

                log.info("📊 [BATCH-STATS] Batch #{} for workspace {} | ⏱️ {}ms processing | 📨 {} total → {} filtered → {} processed | 👥 {} participants | 🏷️ {} unique users | 📺 {} unique channels | 🏢 Tenant: {}",
                    batchNumber, workspace.getName(), batchProcessingTime, messages.size(), filteredCount,
                    slackMessages.size(), participants.size(), uniqueUsers, uniqueChannels, workspace.getTenantId());

                publishEnrichmentRequest(slackMessages, participants, workspace, batchNumber);

                log.info("🚀 [BATCH-PUBLISHED] Batch #{} published to ai-enrich topic | {} messages sent for AI processing",
                    batchNumber, slackMessages.size());

            } else {
                log.warn("⚠️ [BATCH-EMPTY] No valid messages found in batch #{} for workspace: {} after filtering",
                    batchNumber, workspace.getName());
            }
        } catch (Exception e) {
            long batchProcessingTime = System.currentTimeMillis() - batchStartTime;
            log.error("🚨 [BATCH-ERROR] Critical error processing batch #{} after {}ms for workspace {}: {}",
                     batchNumber, batchProcessingTime, workspace.getName(), e.getMessage(), e);
            throw new RuntimeException("Failed to process message batch: " + batchNumber, e);
        }
    }

    /**
     * Filters messages to only include recent ones based on configured age threshold.
     * Old messages are marked as processed to prevent reloading them in future cycles.
     * This prevents processing very old messages that may no longer be relevant.
     */
    private List<Message> filterRecentMessages(final List<Message> messages,
                                               final String workspaceName,
                                               final int batchNumber) {
        if (messages == null || messages.isEmpty()) {
            return new ArrayList<>();
        }

        try {
            // Calculate cutoff timestamp (current time - maxMessageAgeDays)
            final long cutoffEpochSeconds = LocalDateTime.now(ZoneOffset.UTC)
                .minusDays(maxMessageAgeDays)
                .toEpochSecond(ZoneOffset.UTC);

            log.debug("🔍 [MESSAGE-FILTER] Batch #{} filtering messages older than {} days (cutoff: {})",
                batchNumber, maxMessageAgeDays,
                LocalDateTime.ofEpochSecond(cutoffEpochSeconds, 0, ZoneOffset.UTC));

            final List<Message> recentMessages = new ArrayList<>();
            final List<Message> oldMessages = new ArrayList<>();
            int oldMessageCount = 0;
            int invalidTimestampCount = 0;

            for (final Message message : messages) {
                try {
                    final String messageTs = message.getMessageTs();

                    if (messageTs == null || messageTs.trim().isEmpty()) {
                        invalidTimestampCount++;
                        log.debug("⚠️ [MESSAGE-FILTER] Message {} has no timestamp - including by default",
                            message.getId());
                        recentMessages.add(message);
                        continue;
                    }

                    // Use TimestampUtil to parse both timestamp formats
                    final long messageEpochSeconds = TimestampUtil.parseToEpochSeconds(messageTs);

                    if (messageEpochSeconds >= cutoffEpochSeconds) {
                        recentMessages.add(message);
                    } else {
                        oldMessageCount++;
                        oldMessages.add(message);
                        log.debug("⏰ [MESSAGE-FILTER] Excluding old message {} (timestamp: {}, format: {}, {} days old)",
                            message.getId(), messageTs, TimestampUtil.getTimestampFormatDescription(messageTs),
                            (cutoffEpochSeconds - messageEpochSeconds) / 86400);
                    }

                } catch (Exception e) {
                    invalidTimestampCount++;
                    log.warn("⚠️ [MESSAGE-FILTER] Error parsing timestamp for message {}: {} - including by default",
                        message.getId(), e.getMessage());
                    recentMessages.add(message); // Include messages with invalid timestamps
                }
            }

            // Mark old messages as processed to prevent reloading them
            if (!oldMessages.isEmpty()) {
                try {
                    log.info("📝 [OLD-MESSAGE-CLEANUP] Marking {} old messages as processed for workspace {}",
                           oldMessages.size(), workspaceName);
                    slidingWindowService.markMessagesAsProcessed(oldMessages);
                    log.info("✅ [OLD-MESSAGE-CLEANUP] Successfully marked {} old messages as processed",
                           oldMessages.size());
                } catch (Exception e) {
                    log.error("❌ [OLD-MESSAGE-CLEANUP] Failed to mark old messages as processed: {}",
                            e.getMessage(), e);
                }
            }

            // Check if we have sufficient recent messages
            if (recentMessages.size() < minRecentMessages) {
                log.warn("⚠️ [MESSAGE-FILTER] Batch #{} for workspace {} has only {} recent messages (minimum: {}) - {} old (marked processed), {} invalid timestamps",
                    batchNumber, workspaceName, recentMessages.size(), minRecentMessages,
                    oldMessageCount, invalidTimestampCount);
            } else {
                log.info("✅ [MESSAGE-FILTER] Batch #{} for workspace {} filtered: {} recent, {} old (>{}d, marked processed), {} invalid timestamps",
                    batchNumber, workspaceName, recentMessages.size(), oldMessageCount,
                    maxMessageAgeDays, invalidTimestampCount);
            }

            return recentMessages;

        } catch (Exception e) {
            log.error("🚨 [MESSAGE-FILTER] Error filtering messages for batch #{} in workspace {}: {} - processing all messages",
                batchNumber, workspaceName, e.getMessage(), e);
            return new ArrayList<>(messages); // Return all messages on error
        }
    }


    /**
     * Processes all messages in a batch and converts them to SlackMessage format.
     */
    private void processMessagesInBatch(final List<Message> messages,
                                       final List<SlackMessage> slackMessages,
                                       final List<SlackParticipant> participants) {
        for (final var message : messages) {
            try {
                final var slackMessage = convertToSlackMessage(message);
                slackMessages.add(slackMessage);
                addParticipantIfNew(message, participants);
            } catch (Exception e) {
                log.warn("⚠️ Failed to process individual message {}: {}",
                    message.getId(), e.getMessage());
            }
        }

        log.debug("🔄 Processed {} messages resulting in {} slack messages and {} unique participants",
            messages.size(), slackMessages.size(), participants.size());
    }

    /**
     * Converts a Redis Message to a SlackMessage.
     */
    private SlackMessage convertToSlackMessage(final Message message) {
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }

        final var slackMessage = new SlackMessage();

        setMessageCoreFields(slackMessage, message);
        setMessageUserFields(slackMessage, message);
        setMessageMetadata(slackMessage, message);
        setMessageTimestamp(slackMessage, message);
        return slackMessage;
    }

    /**
     * Sets the core message fields on a SlackMessage.
     */
    private void setMessageCoreFields(final SlackMessage slackMessage, final Message message) {
        slackMessage.setId(message.getId());
        slackMessage.setTs(message.getMessageTs());
        slackMessage.setMessageTs(message.getMessageTs());
        slackMessage.setUniqueUserId(message.getUserId());
        slackMessage.setUsername(message.getUsername());
        slackMessage.setText(message.getText());
        slackMessage.setContent(message.getText());
        slackMessage.setChannelId(message.getChannelId());
        slackMessage.setChannelName(message.getChannelName());
        slackMessage.setThreadTs(message.getThreadTs());
        slackMessage.setType(message.getMessageType());
        slackMessage.setMessageType(message.getMessageType());
        slackMessage.setSubtype(message.getSubtype());

        // Set permalink with default value to prevent null exclusion during Kafka serialization
        String permalink = message.getPermaLink();
        if (permalink == null || permalink.trim().isEmpty()) {
            permalink = "deemerge.ai"; // Default value to ensure field is not null
        }
        slackMessage.setPermaLink(permalink);
        // Normalize source to uppercase for consistency
        slackMessage.setSource(message.getSource() != null && !message.getSource().trim().isEmpty()
            ? message.getSource().toUpperCase()
            : "SLACK");

        // Debug: Log permalink setting
        log.debug("🔗 Setting permalink for message {}: {} -> {}",
                  message.getId(), message.getPermaLink(), slackMessage.getPermaLink());

        slackMessage.setTenantId(message.getTenantId());
        slackMessage.setTenantSchema(message.getTenantSchema());
        slackMessage.setWorkspaceId(message.getWorkspaceId());
        slackMessage.setDeemergeUserId(message.getDeemergeUserId());
        slackMessage.setTenantWorkspaceIndex(message.getTenantWorkspaceIndex());
        slackMessage.setTenantWorkspaceChannelIndex(message.getTenantWorkspaceChannelIndex());
        slackMessage.setTenantWorkspaceChannelThreadIndex(message.getTenantWorkspaceChannelThreadIndex());
        slackMessage.setWorkspaceChannelThreadIndex(message.getWorkspaceChannelThreadIndex());
    }

    /**
     * Sets the user profile fields on a SlackMessage with fallback logic for display names.
     */
    private void setMessageUserFields(final SlackMessage slackMessage, final Message message) {
        final String normalizedDisplayName = determineNormalizedDisplayName(message);
        final String displayName = message.getDisplayName() != null && !message.getDisplayName().isEmpty()
            ? message.getDisplayName()
            : normalizedDisplayName;

        // Set uniqueUserId (use uniqueUserId, fallback to slackUserId for backward compatibility)
        String uniqueUserId = message.getUniqueUserId();
        if (uniqueUserId == null || uniqueUserId.trim().isEmpty()) {
            uniqueUserId = message.getSlackUserId();
        }
        slackMessage.setUniqueUserId(uniqueUserId);

        slackMessage.setTeamId(message.getTeamId());
        slackMessage.setName(message.getName());
        slackMessage.setEmailConfirmed(message.getEmailConfirmed());
        slackMessage.setDisplayName(displayName);
        slackMessage.setDisplayNameNormalized(message.getDisplayNameNormalized());
        slackMessage.setRealNameNormalized(message.getRealNameNormalized());
        slackMessage.setEmail(message.getEmail());
        slackMessage.setTitle(message.getTitle());
        slackMessage.setPhone(message.getPhone());
        slackMessage.setFirstName(message.getFirstName());
        slackMessage.setLastName(message.getLastName());
        slackMessage.setPronouns(message.getPronouns());
        slackMessage.setStatusText(message.getStatusText());

        setUserProfileImages(slackMessage, message);

        slackMessage.setTeamName(message.getTeamName());
        slackMessage.setSlackUpdatedAt(message.getSlackUpdatedAt());
    }

    /**
     * Sets all user profile image URLs on a SlackMessage.
     */
    private void setUserProfileImages(final SlackMessage slackMessage, final Message message) {
        slackMessage.setAvatarHash(message.getAvatarHash());

        // Set both avatarUrl (preferred) and imageOriginal (legacy fallback)
        slackMessage.setAvatarUrl(message.getAvatarUrl());
        slackMessage.setImageOriginal(message.getImageOriginal());

        slackMessage.setImage24(message.getImage24());
        slackMessage.setImage32(message.getImage32());
        slackMessage.setImage48(message.getImage48());
        slackMessage.setImage72(message.getImage72());
        slackMessage.setImage192(message.getImage192());
        slackMessage.setImage512(message.getImage512());
        slackMessage.setImage1024(message.getImage1024());
    }

    /**
     * Determines normalized display name with fallback logic.
     */
    private String determineNormalizedDisplayName(final Message message) {
        var normalizedDisplayName = message.getDisplayNameNormalized();

        if (normalizedDisplayName == null || normalizedDisplayName.trim().isEmpty()) {
            normalizedDisplayName = message.getRealNameNormalized();
        }

        if (normalizedDisplayName == null || normalizedDisplayName.trim().isEmpty()) {
            normalizedDisplayName = message.getUsername();
        }

        return normalizedDisplayName != null ? normalizedDisplayName : "";
    }

    /**
     * Sets the metadata fields on a SlackMessage.
     */
    private void setMessageMetadata(final SlackMessage slackMessage, final Message message) {
        slackMessage.setMetadata(message.getMetadata());
        slackMessage.setIngestedAt(message.getIngestedAt());
    }

    /**
     * Sets the timestamp on a SlackMessage with robust parsing and error handling.
     * Supports both Slack format (e.g., "1609459200.123456") and millisecond format (e.g., "1609459200000").
     */
    private void setMessageTimestamp(final SlackMessage slackMessage, final Message message) {
        final String messageTs = message.getMessageTs();

        if (messageTs == null || messageTs.trim().isEmpty()) {
            log.debug("⏰ No timestamp available for message {}", message.getId());
            return;
        }

        try {
            // Use TimestampUtil to handle both timestamp formats
            final LocalDateTime timestamp = TimestampUtil.parseToLocalDateTime(messageTs);

            if (timestamp != null) {
                slackMessage.setTimestamp(timestamp);
                log.debug("✅ [TIMESTAMP-SET] Message {} timestamp set: {} (format: {})",
                    message.getId(), timestamp, TimestampUtil.getTimestampFormatDescription(messageTs));
            } else {
                log.warn("⚠️ [TIMESTAMP-FAILED] Failed to parse timestamp for message {}: '{}'",
                    message.getId(), messageTs);
            }

        } catch (Exception e) {
            log.warn("⚠️ [TIMESTAMP-ERROR] Unexpected error parsing timestamp for message {}: '{}' - {}",
                message.getId(), messageTs, e.getMessage());
        }
    }

    /**
     * Adds a participant to the list if they are not already present.
     * Maintains unique participants by user ID.
     */
    private void addParticipantIfNew(final Message message, final List<SlackParticipant> participants) {
        final String userId = message.getUserId();

        if (userId == null || userId.trim().isEmpty()) {
            log.debug("👤 Skipping participant addition for message {} - no valid user ID", message.getId());
            return;
        }

        final boolean participantExists = participants.stream()
            .anyMatch(p -> userId.equals(p.getId()));

        if (!participantExists) {
            final var participant = createParticipantFromMessage(message);
            participants.add(participant);
        }
    }

    /**
     * Creates a SlackParticipant from a Message.
     */
    private SlackParticipant createParticipantFromMessage(final Message message) {
        final var participant = new SlackParticipant();
        participant.setId(message.getUserId());
        participant.setUsername(message.getUsername());
        participant.setName(message.getName());
        participant.setEmail(message.getEmail());
        participant.setRole(message.getTitle());
        participant.setDisplayName(message.getDisplayName());
        participant.setImageUrl(message.getImage72());
        return participant;
    }

    /**
     * Publishes an enrichment request to the AI message producer.
     */
    private void publishEnrichmentRequest(final List<SlackMessage> slackMessages,
                                        final List<SlackParticipant> participants,
                                        final Workspace workspace,
                                        final int batchNumber) {

        final String conversationId = workspace.getId() + BATCH_CONVERSATION_ID_SEPARATOR + batchNumber;
        final var context = createBatchContext(workspace, batchNumber, slackMessages.size(), participants.size(), slackMessages);
        final String tenantSchema = workspace.getTenantSchema() != null ? workspace.getTenantSchema() : defaultTenantSchema;

        try {
            // For batch processing, use workspace ID as parentId to group all batches from same workspace
            String parentJobId = "workspace-" + workspace.getId();

            aiMessageProducer.scheduleAiProcessing(
                AITaskType.ENRICH_CONVERSATION,
                "", // content - empty for conversation enrichment
                workspace.getTenantId(),
                tenantSchema,
                workspace.getDeemergeUserId(), // userId - not needed for conversation enrichment
                conversationId,
                slackMessages,
                participants,
                context,
                null, // preferredProvider
                null, // replyTopic
                parentJobId // parentId - groups batches under workspace processing
            );

            log.debug("🚀 Successfully published AI enrichment request for conversation: {} with {} messages and {} participants",
                conversationId, slackMessages.size(), participants.size());

        } catch (Exception e) {
            log.error("🚨 Failed to publish AI enrichment request for batch #{} in workspace {}: {}",
                batchNumber, workspace.getName(), e.getMessage(), e);
            throw new RuntimeException("Failed to publish enrichment request", e);
        }
    }

    /**
     * Creates the context map for batch processing.
     */
    private Map<String, Object> createBatchContext(final Workspace workspace,
                                                  final int batchNumber,
                                                  final int messageCount,
                                                  final int participantCount,
                                                  final List<SlackMessage> messages) {
        final var context = new HashMap<String, Object>();

        context.put("workspaceId", workspace.getId());
        context.put("workspaceName", workspace.getName());
        context.put("tenantId", workspace.getTenantId());
        context.put("tenantSchema", workspace.getTenantSchema());
        context.put("teamId", workspace.getTeamId());
        context.put("batchNumber", batchNumber);
        context.put("messageCount", messageCount);
        context.put("participantCount", participantCount);
        context.put("timestamp", LocalDateTime.now(ZoneOffset.UTC));
        context.put("deemergeUserId", workspace.getDeemergeUserId());
        context.put("deemergeUserName", workspace.getDeemergeUserName());

        // Add original message IDs for tracking processed messages
        // This allows PostProcessingConsumer to mark specific messages as processed
        // when AI processing is successful
        List<String> messageIds = messages.stream()
            .map(SlackMessage::getId)
            .collect(Collectors.toList());
        context.put("originalMessageIds", messageIds);

        log.debug("📋 Created batch context for workspace {} batch #{}: {} messages, {} participants, tracking {} message IDs",
            workspace.getName(), batchNumber, messageCount, participantCount, messageIds.size());

        return context;
    }
}
