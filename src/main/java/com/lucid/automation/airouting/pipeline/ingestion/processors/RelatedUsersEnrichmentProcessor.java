package com.lucid.automation.airouting.pipeline.ingestion.processors;

import com.lucid.automation.airouting.pipeline.ingestion.MessageProcessor;
import com.lucid.automation.airouting.pipeline.ingestion.IngestionProcessingContext;
import com.lucid.automation.airouting.pipeline.ProcessingResult;
import com.lucid.automation.airouting.producer.UserChannelProducer;
import com.lucid.automation.airouting.service.UserService;
import com.lucid.automation.airouting.service.WorkspaceService;
import com.lucid.automation.airouting.model.User;
import com.lucid.automation.airouting.model.Workspace;
import com.lucid.automation.common.dto.event.UserChannelEventDTO;
import com.lucid.automation.common.dto.messaging.IngestionEventDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Optional;

/**
 * Processor that enriches related users from message content and publishes them to Kafka.
 * This processor extracts user mentions from message text and creates user-channel relationship events.
 *
 * @author AI Assistant
 */
@Component
public class RelatedUsersEnrichmentProcessor implements MessageProcessor {

    private static final Logger logger = LoggerFactory.getLogger(RelatedUsersEnrichmentProcessor.class);

    private final UserChannelProducer userChannelProducer;
    private final UserService userService;
    private final WorkspaceService workspaceService;

    public RelatedUsersEnrichmentProcessor(UserChannelProducer userChannelProducer, UserService userService, WorkspaceService workspaceService) {
        this.userChannelProducer = userChannelProducer;
        this.userService = userService;
        this.workspaceService = workspaceService;
    }

    @Override
    public ProcessingResult process(IngestionProcessingContext context) {
        IngestionEventDTO ingestionEvent = context.getIngestionEvent();
        try {
            // Extract related users from message content
            List<UserChannelEventDTO.SlackUserInfo> relatedUsers = extractRelatedUsers(ingestionEvent);

            if (relatedUsers.isEmpty()) {
                logger.debug("No related users found in message: {}", context.getMessageId());
                context.setProcessingData("relatedUsersCount", 0);
                return ProcessingResult.success(getProcessorName());
            }

            // Publish related users event to Kafka
            String publishedMessageId = publishRelatedUsersEvent(ingestionEvent, relatedUsers);

            // Store results in context
            context.setProcessingData("relatedUsers", relatedUsers);
            context.setProcessingData("relatedUsersCount", relatedUsers.size());
            context.setProcessingData("userChannelEventMessageId", publishedMessageId);
            return ProcessingResult.success(getProcessorName());

        } catch (Exception e) {
            String errorMsg = "Failed to enrich and publish related users: " + e.getMessage();
            logger.error(errorMsg + " for messageId: {}", context.getMessageId(), e);

            // Store error but don't fail the pipeline
            context.setProcessingData("relatedUsersError", errorMsg);
            context.setProcessingData("relatedUsersCount", 0);

            return ProcessingResult.success(getProcessorName()); // Continue pipeline even if this fails
        }
    }

    /**
     * Extracts related users from the ingestion event message relatedUsers field
     *
     * @param ingestionEvent The ingestion event containing the message
     * @return List of SlackUserInfo for related users
     */
    private List<UserChannelEventDTO.SlackUserInfo> extractRelatedUsers(IngestionEventDTO ingestionEvent) {
        List<UserChannelEventDTO.SlackUserInfo> relatedUsers = new ArrayList<>();

        // Get related users from the message's relatedUsers field
        List<String> messageRelatedUsers = ingestionEvent.getMessage().getRelatedUsers();
        if (messageRelatedUsers == null || messageRelatedUsers.isEmpty()) {
            logger.debug("No related users found in message: {}", ingestionEvent.getMessage().getTs());
            return relatedUsers;
        }

        // Get the message sender ID to exclude from related users
        String senderUserId = ingestionEvent.getUser() != null ?
                            ingestionEvent.getUser().getSlackUserId() : null;
        String tenantId = ingestionEvent.getTenantId();
        String teamId = ingestionEvent.getMessage().getTeamId();

        // Convert related user IDs to SlackUserInfo objects with database lookup
        for (String userId : messageRelatedUsers) {
            if (userId != null && !userId.trim().isEmpty() && !userId.equals(senderUserId)) {

                String userName = userId; // Default to userId if lookup fails

                try {
                    // Lookup user from database to get the actual username/display name
                    Optional<User> userOpt = userService.getUser(tenantId, teamId, userId);
                    if (userOpt.isPresent()) {
                        User user = userOpt.get();
                        // Use display name if available, otherwise use name, otherwise fallback to userId
                        if (user.getDisplayName() != null && !user.getDisplayName().trim().isEmpty()) {
                            userName = user.getDisplayName();
                        } else if (user.getName() != null && !user.getName().trim().isEmpty()) {
                            userName = user.getName();
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Failed to lookup user from database: userId={}, error={}", userId, e.getMessage());
                    // Continue with userId as userName
                }

                UserChannelEventDTO.SlackUserInfo userInfo = UserChannelEventDTO.SlackUserInfo.builder()
                    .slackUserId(userId)
                    .slackUserName(userName)
                    .build();

                relatedUsers.add(userInfo);

                logger.debug("Found related user in message: userId={}, userName={}", userId, userName);
            }
        }

        logger.debug("Extracted {} related users from message: {}",
                    relatedUsers.size(), ingestionEvent.getMessage().getTs());

        return relatedUsers;
    }

    /**
     * Publishes the related users event to Kafka
     *
     * @param ingestionEvent The original ingestion event
     * @param relatedUsers The list of related users to publish
     * @return The message ID of the published event
     */
    private String publishRelatedUsersEvent(IngestionEventDTO ingestionEvent,
                                          List<UserChannelEventDTO.SlackUserInfo> relatedUsers) {

        String tenantId = ingestionEvent.getTenantId();
        String tenantSchema = ingestionEvent.getTenantSchema();

        String userId = ingestionEvent.getUser() != null ?
                       ingestionEvent.getUser().getSlackUserId() : "unknown";
        String teamId = ingestionEvent.getMessage().getTeamId();
        String channelId = ingestionEvent.getMessage().getChannelId();
        String channelName = ingestionEvent.getMessage().getChannelName();
        String teamName = ingestionEvent.getMessage().getTeamName();

        return userChannelProducer.publishRelatedUsersEvent(
            tenantId, tenantSchema, userId, channelId, channelName,
            teamId, teamName, relatedUsers);
    }    @Override
    public String getProcessorName() {
        return "RelatedUsersEnrichmentProcessor";
    }

    @Override
    public int getOrder() {
        return 50; // Last processor in the ingestion pipeline
    }
}
