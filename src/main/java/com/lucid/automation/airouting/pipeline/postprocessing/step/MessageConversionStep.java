package com.lucid.automation.airouting.pipeline.postprocessing.step;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucid.automation.airouting.model.Channel;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.pipeline.postprocessing.PostProcessingContext;
import com.lucid.automation.airouting.service.ChannelService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Pipeline step that converts request maps to SlackMessage objects
 */
@Component
public class MessageConversionStep implements PipelineStep {

    private static final Logger logger = LoggerFactory.getLogger(MessageConversionStep.class);

    private final ObjectMapper objectMapper;
    private final ChannelService channelService;

    public MessageConversionStep(ObjectMapper objectMapper, ChannelService channelService) {
        this.objectMapper = objectMapper;
        this.channelService = channelService;
    }

    @Override
    public PipelineStepResult execute(PostProcessingContext context) {
        logger.debug("Executing message conversion step");

        try {
            Map<String, Object> aiResultMap = context.getAiResultMap();
            if (aiResultMap == null) {
                return PipelineStepResult.failure("AI result map is null");
            }

            // Convert the request objects from LinkedHashMap to SlackMessage objects
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> requestMapList = (List<Map<String, Object>>) aiResultMap.get("request");

            List<SlackMessage> requestMessages;
            try {
                requestMessages = convertToSlackMessages(requestMapList);
                // Log permaLink status for debugging
                long messagesWithPermaLink = requestMessages.stream()
                    .mapToLong(msg -> msg.getPermaLink() != null && !msg.getPermaLink().trim().isEmpty() ? 1 : 0)
                    .sum();
                logger.info("Converted {} request messages, {} have permaLink", requestMessages.size(), messagesWithPermaLink);
            } catch (Exception e) {
                logger.error("Failed to convert request maps to SlackMessage objects: {}", e.getMessage());
                requestMessages = new ArrayList<>();
            }

            context.setRequestMessages(requestMessages);

            // Extract response result
            String responseResult = (String) aiResultMap.get("response");
            context.setResponseResult(responseResult);

            logger.debug("Message conversion completed successfully. Converted {} messages", requestMessages.size());
            return PipelineStepResult.success("Message conversion completed successfully");

        } catch (Exception e) {
            logger.error("Error during message conversion: {}", e.getMessage(), e);
            return PipelineStepResult.failure("Message conversion error: " + e.getMessage(), e);
        }
    }

    /**
     * Convert a list of LinkedHashMap objects to SlackMessage objects
     */
    private List<SlackMessage> convertToSlackMessages(List<Map<String, Object>> requestMapList) {
        if (requestMapList == null) {
            return new ArrayList<>();
        }

        return requestMapList.stream()
            .map(this::convertMapToSlackMessage)
            .map(this::ensureChannelNameResolved)  // Ensure channelName is resolved for all messages
            .collect(Collectors.toList());
    }

    /**
     * Ensure channelName is resolved for a SlackMessage, attempting resolution if missing
     */
    private SlackMessage ensureChannelNameResolved(SlackMessage message) {
        if ((message.getChannelName() == null || message.getChannelName().trim().isEmpty()) &&
            message.getChannelId() != null && !message.getChannelId().trim().isEmpty()) {

            String resolvedChannelName = resolveChannelNameFromId(message.getChannelId());

            if (resolvedChannelName != null && !resolvedChannelName.trim().isEmpty()) {
                message.setChannelName(resolvedChannelName);
                logger.debug("Successfully resolved channelName '{}' for channelId '{}' in message post-processing",
                           resolvedChannelName, message.getChannelId());
            } else {
                logger.warn("Failed to resolve channelName for channelId '{}'", message.getChannelId());
            }
        }
        return message;
    }

    /**
     * Convert a single Map to SlackMessage object
     */
    private SlackMessage convertMapToSlackMessage(Map<String, Object> map) {
        try {
            // Use ObjectMapper to convert Map to SlackMessage
            SlackMessage message = objectMapper.convertValue(map, SlackMessage.class);

            // Defensive check: ensure permaLink is preserved even if ObjectMapper misses it
            if (message.getPermaLink() == null && map.containsKey("permaLink")) {
                String permaLink = (String) map.get("permaLink");
                message.setPermaLink(permaLink);
                logger.debug("Defensively set permaLink for message {}: {}", message.getId(), permaLink);
            }

            return message;
        } catch (Exception e) {
            logger.warn("Failed to convert map to SlackMessage: {}, error: {}", map, e.getMessage());
            // Return a basic SlackMessage with minimal data including permaLink
            return createFallbackSlackMessage(map);
        }
    }

    private SlackMessage createFallbackSlackMessage(Map<String, Object> map) {
        SlackMessage message = new SlackMessage();
        message.setId((String) map.get("id"));
        message.setContent((String) map.get("content"));
        message.setUserId((String) map.get("userId"));
        message.setChannelId((String) map.get("channelId"));

        String channelName = (String) map.get("channelName");
        String channelId = (String) map.get("channelId");

        // Try to resolve channel name if missing but channel ID is present
        if ((channelName == null || channelName.trim().isEmpty()) &&
            channelId != null && !channelId.trim().isEmpty()) {

            channelName = resolveChannelNameFromId(channelId);
            logger.debug("Resolved channelName '{}' for channelId '{}' in fallback conversion", channelName, channelId);
        }

        message.setChannelName(channelName);
        message.setUsername((String) map.get("username"));
        message.setTeamId((String) map.get("teamId"));
        message.setTenantId((String) map.get("tenantId"));
        message.setWorkspaceId((String) map.get("workspaceId"));

        // Always set permaLink in fallback conversion
        String permaLink = (String) map.get("permaLink");
        message.setPermaLink(permaLink);
        String source = (String) map.get("source");
        message.setSource(source != null ? source : "slack");

        // Set additional required fields for complete SlackMessage
        // Set both uniqueUserId (preferred) and slackUserId (legacy) from map
        String uniqueUserId = (String) map.get("uniqueUserId");
        String slackUserId = (String) map.get("slackUserId");
        message.setUniqueUserId(uniqueUserId);
        message.setSlackUserId(slackUserId);

        message.setDisplayName((String) map.get("displayName"));
        message.setText((String) map.get("text"));
        message.setTs((String) map.get("ts"));
        message.setMessageTs((String) map.get("messageTs"));
        message.setThreadTs((String) map.get("threadTs"));

        // Handle timestamp conversion with array support
        Object timestampObj = map.get("timestamp");
        if (timestampObj instanceof List<?> timestampArray) {
            if (timestampArray.size() >= 6) {
                try {
                    int year = ((Number) timestampArray.get(0)).intValue();
                    int month = ((Number) timestampArray.get(1)).intValue();
                    int day = ((Number) timestampArray.get(2)).intValue();
                    int hour = ((Number) timestampArray.get(3)).intValue();
                    int minute = ((Number) timestampArray.get(4)).intValue();
                    int second = ((Number) timestampArray.get(5)).intValue();

                    LocalDateTime timestamp = LocalDateTime.of(year, month, day, hour, minute, second);
                    message.setTimestamp(timestamp);
                } catch (Exception e) {
                    logger.warn("Failed to parse timestamp array in fallback conversion: {}", e.getMessage());
                }
            }
        } else if (timestampObj instanceof String) {
            try {
                LocalDateTime timestamp = LocalDateTime.parse((String) timestampObj);
                message.setTimestamp(timestamp);
            } catch (Exception e) {
                logger.warn("Failed to parse timestamp string in fallback conversion: {}", e.getMessage());
            }
        }

        return message;
    }

    /**
     * Resolves channelName from channelId by querying the database
     */
    private String resolveChannelNameFromId(String channelId) {
        try {
            if (channelId == null || channelId.trim().isEmpty()) {
                logger.warn("Cannot resolve channelName: channelId is null or empty");
                return null;
            }

            Optional<Channel> channelOptional =
                channelService.findByChannelId(channelId.trim());

            if (channelOptional.isPresent()) {
                Channel channel = channelOptional.get();
                return channel.getChannelName();
            } else {
                return channelId; // Return original channelId as fallback
            }

        } catch (Exception e) {
            logger.warn("Error resolving channel name for channelId {}: {}", channelId, e.getMessage());
            return channelId; // Return original channelId as fallback
        }
    }

    @Override
    public String getStepName() {
        return "MessageConversion";
    }

    @Override
    public boolean continueOnFailure() {
        return false; // Stop processing if message conversion fails
    }

    @Override
    public int getExecutionOrder() {
        return 20; // Execute after input validation
    }
}
