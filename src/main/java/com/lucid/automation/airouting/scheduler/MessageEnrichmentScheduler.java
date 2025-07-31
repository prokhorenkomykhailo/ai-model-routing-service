package com.lucid.automation.airouting.scheduler;

import com.lucid.automation.airouting.model.AITaskType;
import com.lucid.automation.airouting.model.Message;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.SlackParticipant;
import com.lucid.automation.airouting.model.Workspace;
import com.lucid.automation.airouting.producer.AIMessageProducer;
import com.lucid.automation.airouting.service.SlidingWindowService;
import com.lucid.automation.airouting.service.WorkspaceService;
import com.lucid.automation.airouting.util.IdUtil;

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

/**
 * Scheduler for enriching messages from data storage service.
 * Fetches workspace information, loads messages for each workspace,
 * and publishes them to the AI enrichment queue using a sliding window approach.
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
    private static final String TIMESTAMP_SEPARATOR = "\\.";
    private static final int TIMESTAMP_EPOCH_INDEX = 0;

    // Dependencies
    private final AIMessageProducer aiMessageProducer;
    private final WorkspaceService workspaceService;
    private final SlidingWindowService slidingWindowService;
    private final ApplicationContext applicationContext;
    private final int batchSize;
    private final String defaultTenantSchema;

    /**
     * Constructor with dependency and configuration injection.
     */
    public MessageEnrichmentScheduler(
            final AIMessageProducer aiMessageProducer,
            final WorkspaceService workspaceService,
            final SlidingWindowService slidingWindowService,
            final ApplicationContext applicationContext,
            @Value("${ai.enrichment.scheduler.batch-size:" + DEFAULT_BATCH_SIZE + "}") final int batchSize,
            @Value("${ai.enrichment.scheduler.default-tenant-schema:" + DEFAULT_TENANT_SCHEMA + "}") final String defaultTenantSchema) {

        this.aiMessageProducer = aiMessageProducer;
        this.workspaceService = workspaceService;
        this.slidingWindowService = slidingWindowService;
        this.applicationContext = applicationContext;
        this.batchSize = batchSize;
        this.defaultTenantSchema = defaultTenantSchema;
    }

    @PostConstruct
    public void postConstruct() {
        log.info("=== MessageEnrichmentScheduler @PostConstruct Called ===");

        // Check if scheduling is enabled globally
        try {
            String[] schedulingBeans = applicationContext.getBeanNamesForAnnotation(org.springframework.scheduling.annotation.EnableScheduling.class);
            log.info("@EnableScheduling beans found: {}", java.util.Arrays.toString(schedulingBeans));
        } catch (Exception e) {
            log.warn("Error checking @EnableScheduling beans: {}", e.getMessage());
        }

        // Check property values
        try {
            org.springframework.core.env.Environment env = applicationContext.getEnvironment();
            String enabledProperty = env.getProperty("ai.enrichment.scheduler.enabled");
            String cronProperty = env.getProperty("ai.enrichment.scheduler.cron");

            log.info("ai.enrichment.scheduler.enabled property: {}", enabledProperty);
            log.info("ai.enrichment.scheduler.cron property: {}", cronProperty);
            log.info("AI_ENRICHMENT_ENABLED env var: {}", env.getProperty("AI_ENRICHMENT_ENABLED"));
            log.info("AI_ENRICHMENT_CRON env var: {}", env.getProperty("AI_ENRICHMENT_CRON"));
        } catch (Exception e) {
            log.warn("Error checking environment properties: {}", e.getMessage());
        }

        // Check if this bean is being created
        log.info("MessageEnrichmentScheduler bean successfully created and initialized");
        log.info("=== MessageEnrichmentScheduler @PostConstruct Completed ===");
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("=== MessageEnrichmentScheduler afterPropertiesSet Called ===");
        log.info("All dependencies have been injected successfully");

        // Schedule a test run in 10 seconds to verify scheduling works
        log.info("Scheduler will attempt to run every 5 minutes according to cron expression");
        log.info("Next scheduled execution should occur at the next 5-minute interval");
        log.info("=== MessageEnrichmentScheduler afterPropertiesSet Completed ===");
    }

    /**
     * Scheduled method to process message enrichment for all workspaces.
     */
    @Scheduled(cron = "${ai.enrichment.scheduler.cron:0 */5 * * * ?}")
    public void processMessageEnrichment() {
        log.info("=== SCHEDULER EXECUTED ===");
        log.info("Current time: {}", LocalDateTime.now());
        log.info("Thread: {}", Thread.currentThread().getName());
        log.info("Starting scheduled message enrichment process");

        try {
            final var workspaces = workspaceService.getAllWorkspaces();

            if (workspaces.isEmpty()) {
                log.info("No workspaces found, skipping enrichment cycle");
                return;
            }

            processAllWorkspaces(workspaces);
            log.info("Completed scheduled message enrichment process for {} workspaces", workspaces.size());

        } catch (Exception e) {
            log.error("Critical error during scheduled message enrichment process: {}", e.getMessage(), e);
        }

        log.info("=== SCHEDULER EXECUTION COMPLETED ===");
    }

    /**
     * Processes all workspaces for message enrichment with individual error handling.
     */
    private void processAllWorkspaces(final List<Workspace> workspaces) {
        var successCount = 0;
        var failureCount = 0;

        for (final var workspace : workspaces) {
            try {
                String parentId = IdUtil.generateId("parent_");
                final var results = processWorkspace(workspace, parentId);
                final int workspaceBatches = results[0];
                final int workspaceMessages = results[1];

                log.info("Successfully processed workspace: {} - batches: {}, messages: {}",
                    workspace.getName(), workspaceBatches, workspaceMessages);
                successCount++;

            } catch (Exception e) {
                log.error("Error processing workspace {}: {}", workspace.getName(), e.getMessage(), e);
                failureCount++;
            }
        }

        log.info("Workspace processing summary: {} successful, {} failed out of {} total",
            successCount, failureCount, workspaces.size());
    }

    /**
     * Processes messages for a specific workspace using SlidingWindowService.
     */
    private int[] processWorkspace(final Workspace workspace, final String parentId) {
        log.info("Starting message processing for workspace: {} using SlidingWindowService", workspace.getName());

        final var batchCount = new AtomicInteger(0);
        final var totalProcessed = new AtomicInteger(0);
        final Function<List<Message>, Void> enrichmentProcessor = createEnrichmentProcessor(workspace, batchCount, totalProcessed, parentId);

        try {
            final int messagesProcessed = slidingWindowService.processMessages(
                workspace, batchSize, DEFAULT_SLIDING_WINDOW_SIZE, enrichmentProcessor);

            log.info("Completed processing workspace: {} - {} batches, {} messages processed",
                workspace.getName(), batchCount.get(), messagesProcessed);

            return new int[]{batchCount.get(), messagesProcessed};

        } catch (Exception e) {
            log.error("Critical error processing workspace: {}. Error: {}", workspace.getName(), e.getMessage(), e);
            throw new RuntimeException("Failed to process workspace: " + workspace.getName(), e);
        }
    }

    /**
     * Creates the enrichment processor function for handling message batches.
     */
    private Function<List<Message>, Void> createEnrichmentProcessor(final Workspace workspace,
                                                                   final AtomicInteger batchCount,
                                                                   final AtomicInteger totalProcessed,
                                                                   final String parentId) {
        return messages -> {
            final int currentBatch = batchCount.incrementAndGet();
            log.info("Processing batch #{} with {} messages for workspace: {}",
                currentBatch, messages.size(), workspace.getName());
            try {
                processMessageBatchForEnrichment(messages, workspace, currentBatch, parentId);
                totalProcessed.addAndGet(messages.size());
                log.debug("Successfully processed batch #{} for workspace: {}",
                    currentBatch, workspace.getName());
            } catch (Exception e) {
                log.error("Error processing batch #{} for workspace {}: {}",
                    currentBatch, workspace.getName(), e.getMessage(), e);
            }
            return null;
        };
    }

    /**
     * Process a batch of messages from Redis for AI enrichment.
     * Converts Redis Message objects to SlackMessage format and processes them as a batch.
     */
    private void processMessageBatchForEnrichment(final List<Message> messages,
                                                 final Workspace workspace,
                                                 final int batchNumber,
                                                 final String parentId) {
        if (messages == null || messages.isEmpty()) {
            log.debug("No messages to process in batch #{} for workspace: {}", batchNumber, workspace.getName());
            return;
        }

        try {
            final var slackMessages = new ArrayList<SlackMessage>();
            final var participants = new ArrayList<SlackParticipant>();

            processMessagesInBatch(messages, slackMessages, participants);

            if (!slackMessages.isEmpty()) {
                publishEnrichmentRequest(slackMessages, participants, workspace, batchNumber, parentId);
            } else {
                log.warn("No valid messages found in batch #{} for workspace: {}", batchNumber, workspace.getName());
            }
        } catch (Exception e) {
            log.error("Critical error processing message batch #{} for workspace {}: {}",
                     batchNumber, workspace.getName(), e.getMessage(), e);
            throw new RuntimeException("Failed to process message batch: " + batchNumber, e);
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
                log.warn("Failed to process individual message {}: {}",
                    message.getId(), e.getMessage());
            }
        }

        log.debug("Processed {} messages resulting in {} slack messages and {} unique participants",
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
        slackMessage.setUserId(message.getUserId());
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
        slackMessage.setSource(message.getSource() != null ? message.getSource() : "slack");

        // Debug: Log permalink setting
        log.debug("Setting permalink for message {}: {} -> {}",
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

        slackMessage.setSlackUserId(message.getSlackUserId());
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
     * Expected format is "epoch_seconds.microseconds" (e.g., "1609459200.123456").
     */
    private void setMessageTimestamp(final SlackMessage slackMessage, final Message message) {
        final String messageTs = message.getMessageTs();

        if (messageTs == null || messageTs.trim().isEmpty()) {
            log.debug("No timestamp available for message {}", message.getId());
            return;
        }

        try {
            final String[] timestampParts = messageTs.split(TIMESTAMP_SEPARATOR);
            final long epochSeconds = Long.parseLong(timestampParts[TIMESTAMP_EPOCH_INDEX]);

            final var timestamp = LocalDateTime.ofEpochSecond(epochSeconds, 0, ZoneOffset.UTC);
            slackMessage.setTimestamp(timestamp);
        } catch (NumberFormatException e) {
            log.warn("Invalid timestamp format for message {}: '{}' - {}",
                message.getId(), messageTs, e.getMessage());
        } catch (ArrayIndexOutOfBoundsException e) {
            log.warn("Malformed timestamp structure for message {}: '{}' - {}",
                message.getId(), messageTs, e.getMessage());
        } catch (Exception e) {
            log.warn("Unexpected error parsing timestamp for message {}: '{}' - {}",
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
            log.debug("Skipping participant addition for message {} - no valid user ID", message.getId());
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
                                        final int batchNumber,
                                        final String parentId) {

        final String conversationId = workspace.getId() + BATCH_CONVERSATION_ID_SEPARATOR + batchNumber;
        final var context = createBatchContext(workspace, batchNumber, slackMessages.size(), participants.size());
        final String tenantSchema = workspace.getTenantSchema() != null ? workspace.getTenantSchema() : defaultTenantSchema;

        try {
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
                null,  // replyTopic
                parentId
            );

            log.debug("Successfully published AI enrichment request for conversation: {} with {} messages and {} participants",
                conversationId, slackMessages.size(), participants.size());

        } catch (Exception e) {
            log.error("Failed to publish AI enrichment request for batch #{} in workspace {}: {}",
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
                                                  final int participantCount) {
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

        log.debug("Created batch context for workspace {} batch #{}: {} messages, {} participants",
            workspace.getName(), batchNumber, messageCount, participantCount);

        return context;
    }
}
