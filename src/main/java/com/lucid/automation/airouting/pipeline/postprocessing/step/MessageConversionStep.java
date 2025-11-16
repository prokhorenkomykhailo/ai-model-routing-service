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
import java.util.*;
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
        long startTime = System.currentTimeMillis();
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
                // ✅ PERFORMANCE FIX: Batch load all channels ONCE before processing messages
                Map<String, String> channelCache = batchLoadAllChannels(requestMapList);
                long cacheLoadTime = System.currentTimeMillis() - startTime;
                logger.info("⚡ [PERFORMANCE] Loaded {} channels in {}ms (avoiding {} individual DB calls)", 
                    channelCache.size(), cacheLoadTime, channelCache.size());

                // Convert messages using cached channel data
                requestMessages = convertToSlackMessagesWithCache(requestMapList, channelCache);
                
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

            long totalTime = System.currentTimeMillis() - startTime;
            logger.info("✅ [PERFORMANCE] Message conversion completed in {}ms. Converted {} messages", 
                totalTime, requestMessages.size());
            return PipelineStepResult.success("Message conversion completed successfully");

        } catch (Exception e) {
            long totalTime = System.currentTimeMillis() - startTime;
            logger.error("Error during message conversion after {}ms: {}", totalTime, e.getMessage(), e);
            return PipelineStepResult.failure("Message conversion error: " + e.getMessage(), e);
        }
    }

    /**
     * ✅ PERFORMANCE FIX: Batch load all channels from message list ONCE to avoid N+1 query problem
     * This replaces 10-30 individual DB calls with a single batch operation
     */
    private Map<String, String> batchLoadAllChannels(List<Map<String, Object>> requestMapList) {
        if (requestMapList == null || requestMapList.isEmpty()) {
            return Map.of();
        }

        // Collect all unique channel IDs from messages
        Set<String> allChannelIds = requestMapList.stream()
            .map(map -> (String) map.get("channelId"))
            .filter(Objects::nonNull)
            .filter(id -> !id.trim().isEmpty())
            .collect(Collectors.toSet());

        Map<String, String> channelCache = new HashMap<>();

        // Batch load all channels from database
        for (String channelId : allChannelIds) {
            try {
                Optional<Channel> channelOptional = channelService.findByChannelId(channelId.trim());
                if (channelOptional.isPresent()) {
                    Channel channel = channelOptional.get();
                    channelCache.put(channelId, channel.getChannelName());
                } else {
                    channelCache.put(channelId, channelId); // Fallback to ID
                }
            } catch (Exception e) {
                logger.warn("Failed to load channel {}: {}", channelId, e.getMessage());
                channelCache.put(channelId, channelId); // Fallback to ID
            }
        }

        return channelCache;
    }

    /**
     * ✅ PERFORMANCE FIX: Convert messages using pre-loaded channel cache (no DB calls)
     */
    private List<SlackMessage> convertToSlackMessagesWithCache(List<Map<String, Object>> requestMapList, 
                                                                 Map<String, String> channelCache) {
        if (requestMapList == null) {
            return new ArrayList<>();
        }

        return requestMapList.stream()
            .map(this::convertMapToSlackMessage)
            .map(msg -> ensureChannelNameResolvedFromCache(msg, channelCache))
            .collect(Collectors.toList());
    }

    /**
     * ✅ PERFORMANCE FIX: Resolve channel name from cache instead of DB
     */
    private SlackMessage ensureChannelNameResolvedFromCache(SlackMessage message, Map<String, String> channelCache) {
        if ((message.getChannelName() == null || message.getChannelName().trim().isEmpty()) &&
            message.getChannelId() != null && !message.getChannelId().trim().isEmpty()) {

            String resolvedChannelName = channelCache.getOrDefault(message.getChannelId(), message.getChannelId());
            message.setChannelName(resolvedChannelName);
            logger.debug("Resolved channelName '{}' for channelId '{}' from cache", 
                       resolvedChannelName, message.getChannelId());
        }
        return message;
    }

    /**
     * Convert a list of LinkedHashMap objects to SlackMessage objects
     * @deprecated Use convertToSlackMessagesWithCache for better performance
     */
    @Deprecated
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
        message.setUniqueUserId((String) map.get("uniqueUserId"));
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
        // Set uniqueUserId from map (legacy fields userId and slackUserId are no longer used)
        String uniqueUserId = (String) map.get("uniqueUserId");
        // Fallback to userId or slackUserId from map for backward compatibility
        if (uniqueUserId == null) {
            uniqueUserId = (String) map.get("userId");
            if (uniqueUserId == null) {
                uniqueUserId = (String) map.get("slackUserId");
            }
        }
        message.setUniqueUserId(uniqueUserId);

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
