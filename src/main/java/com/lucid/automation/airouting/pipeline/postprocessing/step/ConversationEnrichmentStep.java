package com.lucid.automation.airouting.pipeline.postprocessing.step;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucid.automation.common.dto.enrichment.ConversationEnrichment;
import com.lucid.automation.common.dto.enrichment.EnrichmentUserDTO;
import com.lucid.automation.common.dto.enrichment.ForwardInfo;
import com.lucid.automation.common.dto.enrichment.SuggestedReply;
import com.lucid.automation.common.dto.enrichment.SummaryPerPerson;
import com.lucid.automation.common.dto.enrichment.TopicEnrichment;
import com.lucid.automation.common.dto.enrichment.UrgencyLevel;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.pipeline.postprocessing.PostProcessingContext;
import com.lucid.automation.airouting.util.json.JsonCleaner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Pipeline step that parses conversation enrichment from AI response
 */
@Component
public class ConversationEnrichmentStep implements PipelineStep {

    private static final Logger logger = LoggerFactory.getLogger(ConversationEnrichmentStep.class);

    private static final String DEFAULT_TOPIC_TITLE = "General Discussion";
    private static final String DEFAULT_SUMMARY = "No summary available";
    private static final String DEFAULT_DETAILED_SUMMARY = "No detailed summary available";
    private static final String DEFAULT_ACTION = "No action suggested";
    private static final String DEFAULT_CATEGORY = "General";
    private static final String ERROR_FAILED_CONVERSATION_ANALYSIS = "Failed to analyze conversation";

    private final ObjectMapper objectMapper;

    public ConversationEnrichmentStep(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public PipelineStepResult execute(PostProcessingContext context) {
        logger.debug("Executing conversation enrichment step");

        try {
            String responseResult = context.getResponseResult();
            List<SlackMessage> messages = context.getRequestMessages();

            if (responseResult == null) {
                logger.warn("Response result is null, using default conversation enrichment");
                context.setConversationEnrichment(getDefaultConversationEnrichment());
                return PipelineStepResult.success("Used default conversation enrichment due to null response");
            }

            ConversationEnrichment enrichment = parseConversationEnrichmentResponse(responseResult, messages);
            context.setConversationEnrichment(enrichment);

            logger.debug("Conversation enrichment completed successfully. Found {} topics",
                        enrichment.topics().size());
            return PipelineStepResult.success("Conversation enrichment completed successfully");

        } catch (Exception e) {
            logger.error("Error during conversation enrichment: {}", e.getMessage(), e);
            // Set default enrichment on error
            context.setConversationEnrichment(getDefaultConversationEnrichment());
            return PipelineStepResult.success("Used default conversation enrichment due to error: " + e.getMessage());
        }
    }

    private ConversationEnrichment parseConversationEnrichmentResponse(String response, List<SlackMessage> messages) {
        try {
            String cleanedResponse = JsonCleaner.cleanJsonResponse(response);
            logger.info("[==>>> GEMINI-PARSE]: Cleaned response:\n{}", cleanedResponse);

            if (cleanedResponse.trim().startsWith("[")) {
                return parseTopicsFromText(cleanedResponse, messages);
            }

            return new ConversationEnrichment(
                List.of(), // No topics
                List.of(), // No action items
                List.of(), // No messages
                null // No urgency level
            );
        } catch (Exception e) {
            logger.error("GEMINI-PARSE: Failed to parse conversation enrichment response. Error: {}", e.getMessage(), e);
            return getDefaultConversationEnrichment();
        }
    }

    private ConversationEnrichment parseTopicsFromText(String topicsText, List<SlackMessage> messages)
        throws JsonMappingException, JsonProcessingException {

        List<?> topicsArray = objectMapper.readValue(topicsText, List.class);
        List<TopicEnrichment> topics = new ArrayList<>();
        for (int i = 0; i < topicsArray.size(); i++) {
            Object topicObj = topicsArray.get(i);
            if (topicObj instanceof Map<?, ?> topicMap) {
                // This would be handled by TopicEnrichmentStep in a real implementation
                // For now, create a basic topic
                TopicEnrichment topic = createBasicTopic(topicMap, messages);
                topics.add(topic);
                logger.info("GEMINI-PARSE: Successfully parsed topic {}: '{}'", i, topic.title());
            }
        }
        return new ConversationEnrichment(topics, List.of(), List.of(), null);
    }

    private TopicEnrichment createBasicTopic(Map<?, ?> topicMap, List<SlackMessage> messages) {
        String title = extractStringValue(topicMap, "title", "Untitled Topic");
        String shortSummary = extractStringValue(topicMap, "shortSummary", DEFAULT_SUMMARY);
        String fullSummary = extractStringValue(topicMap, "fullSummary", DEFAULT_DETAILED_SUMMARY);
        String suggestedAction = extractStringValue(topicMap, "suggestedAction", DEFAULT_ACTION);
        String category = extractStringValue(topicMap, "category", DEFAULT_CATEGORY);
        String subCategory = extractStringValue(topicMap, "subCategory", null);
        String clientOrSupplier = extractStringValue(topicMap, "clientOrSupplier", null);
        String urgencyStr = extractStringValue(topicMap, "urgency", "low");
        String periodStartDate = extractStringValue(topicMap, "periodStartDate", null);
        String periodEndDate = extractStringValue(topicMap, "periodEndDate", null);
        String latestMessageDate = extractStringValue(topicMap, "latestMessageDate", null);

        // Map urgency string to enum
        UrgencyLevel urgency = mapStringToUrgency(urgencyStr);

        // Extract deadline (could be a date string)
        LocalDateTime deadline = parseLocalDateTime(extractStringValue(topicMap, "deadline", null));

        // Calculate startTime, endTime and lastUpdated from messages and AI response
        LocalDateTime startTime = null;
        LocalDateTime endTime = null;
        LocalDateTime lastUpdated = null;

        // First try to extract from AI response if provided
        String aiStartTime = extractStringValue(topicMap, "startTime", null);
        String aiEndTime = extractStringValue(topicMap, "endTime", null);
        String aiLastUpdated = extractStringValue(topicMap, "lastUpdated", null);
        String aiLatestMessageDate = extractStringValue(topicMap, "latestMessageDate", null);

        // Parse AI-provided times if available
        if (aiStartTime != null) {
            startTime = parseLocalDateTime(aiStartTime);
        }
        if (aiEndTime != null) {
            endTime = parseLocalDateTime(aiEndTime);
        }
        if (aiLastUpdated != null) {
            lastUpdated = parseLocalDateTime(aiLastUpdated);
        }

        // Use AI's latestMessageDate as fallback for endTime if not provided
        if (endTime == null && aiLatestMessageDate != null) {
            endTime = parseLocalDateTime(aiLatestMessageDate);
        }

        // Consider AI's periodStartDate and periodEndDate as additional fallbacks
        if (startTime == null && periodStartDate != null) {
            startTime = parseLocalDateTime(periodStartDate);
        }
        if (endTime == null && periodEndDate != null) {
            endTime = parseLocalDateTime(periodEndDate);
        }

        // Extract people involved
        List<EnrichmentUserDTO> peopleInvolved = extractPeopleInvolved(topicMap);
        // Extract lastMessageDatePerPerson
        Map<String, String> lastMessageDatePerPerson = extractStringMap(topicMap, "lastMessageDatePerPerson");

        // Calculate lastUpdated from lastMessageDatePerPerson - find the latest date
        if (lastUpdated == null && !lastMessageDatePerPerson.isEmpty()) {
            lastUpdated = lastMessageDatePerPerson.values().stream()
                .map(this::parseLocalDateTime)
                .filter(date -> date != null)
                .max(LocalDateTime::compareTo)
                .orElse(null);
        }

        // Extract summaryPerPerson from the AI response
        List<SummaryPerPerson> summaryPerPerson = extractSummaryPerPerson(topicMap, lastMessageDatePerPerson);

        // Extract suggested replies
        List<SuggestedReply> suggestedReplies = extractSuggestedReplies(topicMap);

        // Extract suggested forward recipient
        ForwardInfo suggestedForwardRecipient = extractForwardInfo(topicMap);

        return new TopicEnrichment(
            title,
            shortSummary,
            fullSummary,
            suggestedAction,
            clientOrSupplier,
            deadline,
            urgency,
            category,
            subCategory,
            startTime, // now calculated from messages
            endTime,   // now calculated from messages
            periodStartDate,
            periodEndDate,
            latestMessageDate,
            lastUpdated, // now calculated from messages
            peopleInvolved,
            summaryPerPerson,
            lastMessageDatePerPerson,
            suggestedReplies,
            suggestedForwardRecipient
        );
    }

    private String extractStringValue(Map<?, ?> map, String key, String defaultValue) {
        Object value = map.get(key);
        if (value == null) return defaultValue;
        if (value instanceof String str) return str;
        // Convert numbers, booleans, etc. to string
        return value.toString();
    }

    /**
     * Extract summaryPerPerson from the AI response topic map
     */
private List<SummaryPerPerson> extractSummaryPerPerson(Map<?, ?> topicMap, Map<String, String> lastMessageDatePerPerson) {
    Object summaryPerPersonObj = topicMap.get("summaryPerPerson");
    if (summaryPerPersonObj instanceof Map<?, ?> summaryMap) {
        return extractSummaryPerPersonFromMap(summaryMap, lastMessageDatePerPerson);
    } else if (summaryPerPersonObj instanceof List<?> summaryList) {
        return extractSummaryPerPersonFromList(summaryList, lastMessageDatePerPerson);
    }
    return List.of();
}

private List<SummaryPerPerson> extractSummaryPerPersonFromMap(Map<?, ?> summaryMap, Map<String, String> lastMessageDatePerPerson) {
    List<SummaryPerPerson> summaries = new ArrayList<>();
    for (Map.Entry<?, ?> entry : summaryMap.entrySet()) {
        String userId = String.valueOf(entry.getKey());
        String summary = String.valueOf(entry.getValue());
        String lastMessageDateStr = lastMessageDatePerPerson.get(userId);
        LocalDateTime lastMessageDate = parseLocalDateTime(lastMessageDateStr);
        SummaryPerPerson summaryPerPerson = new SummaryPerPerson(
            userId,
            userId, // username - fallback to userId
            userId, // displayName - fallback to userId
            null,   // imageUrl
            summary,
            0,      // messageCount
            null,   // firstMessageDate
            lastMessageDate, // lastMessageDate from lastMessageDatePerPerson
            List.of(), // keyContributions
            List.of(), // actionItems
            List.of()  // sources
        );
        summaries.add(summaryPerPerson);
    }
    return summaries;
}

private List<SummaryPerPerson> extractSummaryPerPersonFromList(List<?> summaryList, Map<String, String> lastMessageDatePerPerson) {
    List<SummaryPerPerson> summaries = new ArrayList<>();
    for (Object summaryObj : summaryList) {
        if (summaryObj instanceof Map<?, ?> summaryObjMap) {
            String id = extractStringValue(summaryObjMap, "id", null);
            String username = extractStringValue(summaryObjMap, "username", null);
            String displayName = extractStringValue(summaryObjMap, "displayName", null);
            String imageUrl = extractStringValue(summaryObjMap, "imageUrl", null);
            String summary = extractStringValue(summaryObjMap, "summary", DEFAULT_SUMMARY);
            Integer messageCount = extractIntegerValue(summaryObjMap, "messageCount", 0);
            String lastMessageDateStr = lastMessageDatePerPerson.get(id);
            LocalDateTime lastMessageDate = parseLocalDateTime(lastMessageDateStr);
            if (id != null) {
                SummaryPerPerson summaryPerPerson = new SummaryPerPerson(
                    id,
                    username != null ? username : id,
                    displayName != null ? displayName : id,
                    imageUrl,
                    summary,
                    messageCount,
                    null, // firstMessageDate
                    lastMessageDate, // lastMessageDate from lastMessageDatePerPerson
                    List.of(), // keyContributions
                    List.of(), // actionItems
                    List.of()  // sources
                );
                summaries.add(summaryPerPerson);
            }
        }
    }
    return summaries;
}

    private Integer extractIntegerValue(Map<?, ?> map, String key, Integer defaultValue) {
        Object value = map.get(key);
        if (value instanceof Number num) {
            return num.intValue();
        }
        return defaultValue;
    }

    /**
     * Map urgency string to UrgencyLevel enum
     */
    private UrgencyLevel mapStringToUrgency(String urgencyStr) {
        if (urgencyStr == null) return UrgencyLevel.LOW;

        return switch (urgencyStr.toLowerCase().trim()) {
            case "critical", "urgent" -> UrgencyLevel.CRITICAL;
            case "high" -> UrgencyLevel.HIGH;
            case "medium", "med" -> UrgencyLevel.MEDIUM;
            case "low" -> UrgencyLevel.LOW;
            default -> {
                logger.debug("Unknown urgency value '{}', defaulting to LOW", urgencyStr);
                yield UrgencyLevel.LOW;
            }
        };
    }

    /**
     * Parse deadline string into LocalDateTime
     */
private LocalDateTime parseLocalDateTime(String dateTimeStr) {
    if (dateTimeStr == null || dateTimeStr.trim().isEmpty()) {
        return null;
    }

    try {
        // Try to parse as ISO date-time string first
        return LocalDateTime.parse(dateTimeStr);
    } catch (Exception e) {
        try {
            // Try to parse as date-only string (e.g., "2025-05-10")
            if (dateTimeStr.matches("\\d{4}-\\d{2}-\\d{2}")) {
                return LocalDateTime.parse(dateTimeStr + "T00:00:00");
            }
        } catch (Exception e2) {
            logger.debug("Failed to parse date as date-only '{}': {}", dateTimeStr, e2.getMessage());
        }
        logger.debug("Failed to parse date '{}': {}", dateTimeStr, e.getMessage());
        return null;
    }
}

    /**
     * Extract people involved as list of EnrichmentUserDTO
     */
    private List<EnrichmentUserDTO> extractPeopleInvolved(Map<?, ?> topicMap) {
        Object value = topicMap.get("peopleInvolved");
        if (value instanceof List<?> list) {
            List<EnrichmentUserDTO> result = new ArrayList<>();
            for (Object item : list) {
                if (item instanceof String userId) {
                    // Create basic EnrichmentUserDTO with just the userId
                    // The TopicEnrichmentStep can enhance this with full user data later
                    EnrichmentUserDTO userDTO = new EnrichmentUserDTO(
                        userId,
                        userId, // username - fallback to userId
                        userId, // displayName - fallback to userId
                        null    // imageUrl
                    );
                    result.add(userDTO);
                }
            }
            return result;
        }
        return List.of();
    }

    /**
     * Extract string map from topic map
     */
    private Map<String, String> extractStringMap(Map<?, ?> topicMap, String key) {
        Object value = topicMap.get(key);
        if (value instanceof Map<?, ?> map) {
            Map<String, String> result = new java.util.HashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                String keyStr = String.valueOf(entry.getKey());
                String valueStr = String.valueOf(entry.getValue());
                result.put(keyStr, valueStr);
            }
            return result;
        }
        return Map.of();
    }

    /**
     * Extract suggested replies from topic map
     */
    private List<SuggestedReply> extractSuggestedReplies(Map<?, ?> topicMap) {
        Object value = topicMap.get("suggestedReplies");
        if (value instanceof List<?> list) {
            List<SuggestedReply> result = new ArrayList<>();
            for (Object item : list) {
                if (item instanceof Map<?, ?> replyMap) {
                    String tone = extractStringValue(replyMap, "tone", "pro");
                    String replyMethod = extractStringValue(replyMap, "replyMethod", "slack");
                    String recipientHandle = extractStringValue(replyMap, "recipientHandle", null);

                    // Handle channel mapping: AI often puts channel ID in "channelName" field
                    String aiChannelName = extractStringValue(replyMap, "channelName", null);
                    String aiChannelId = extractStringValue(replyMap, "channelId", null);

                    // Determine actual channelId and channelName
                    String channelId = null;
                    String channelName = null;

                    if (aiChannelId != null) {
                        // If AI provides channelId, use it directly
                        channelId = aiChannelId;
                        channelName = aiChannelName; // This would be the actual name
                    } else if (aiChannelName != null) {
                        // If AI only provides channelName, check if it looks like an ID
                        if (aiChannelName.startsWith("C") && aiChannelName.length() > 8) {
                            // Looks like a channel ID (format: C023SDG9PDH)
                            channelId = aiChannelName;
                            channelName = null; // To be resolved later by TopicEnrichmentStep
                        } else {
                            // Actual channel name
                            channelName = aiChannelName;
                            channelId = null;
                        }
                    }

                    String threadId = extractStringValue(replyMap, "threadId", null);
                    String to = extractStringValue(replyMap, "to", null);
                    String subject = extractStringValue(replyMap, "subject", null);
                    String messageBody = extractStringValue(replyMap, "messageBody", null);

                    if (messageBody != null) {
                        SuggestedReply reply = new SuggestedReply(
                            tone,
                            replyMethod,
                            recipientHandle,
                            channelName,
                            channelId,
                            threadId,
                            to,
                            List.of(), // cc - empty list for now
                            subject,
                            messageBody
                        );
                        result.add(reply);
                    }
                }
            }
            return result;
        }
        return List.of();
    }

    /**
     * Extract forward info from topic map
     */
    private ForwardInfo extractForwardInfo(Map<?, ?> topicMap) {
        Object value = topicMap.get("suggestedForwardRecipient");
        if (value instanceof Map<?, ?> forwardMap) {
            String channel = extractStringValue(forwardMap, "channel", null);
            String to = extractStringValue(forwardMap, "to", null);
            String subject = extractStringValue(forwardMap, "subject", null);
            String body = extractStringValue(forwardMap, "body", null);

            if (to != null || channel != null) {
                return new ForwardInfo(channel, to, subject, body);
            }
        }
        return null;
    }

    private ConversationEnrichment getDefaultConversationEnrichment() {
        TopicEnrichment defaultTopic = new TopicEnrichment(
            DEFAULT_TOPIC_TITLE,
            DEFAULT_SUMMARY,
            DEFAULT_DETAILED_SUMMARY,
            DEFAULT_ACTION,
            null, // clientOrSupplier
            null, // deadline
            UrgencyLevel.LOW,
            DEFAULT_CATEGORY, // category
            null, // subCategory
            null, // startTime
            null, // endTime
            null, // periodStartDate
            null, // periodEndDate
            null, // latestMessageDate
            null, // lastUpdated
            List.of(), // peopleInvolved
            List.of(), // summaryPerPerson
            Map.of(), // lastMessageDatePerPerson
            List.of(), // suggestedReplies
            null // suggestedForwardRecipient
        );

        return new ConversationEnrichment(
            List.of(defaultTopic),
            List.of(),
            List.of(),
            Map.of("error", ERROR_FAILED_CONVERSATION_ANALYSIS)
        );
    }

    @Override
    public String getStepName() {
        return "ConversationEnrichment";
    }

    @Override
    public boolean continueOnFailure() {
        return true; // Continue even if enrichment fails, use defaults
    }

    @Override
    public int getExecutionOrder() {
        return 40; // Execute after timestamp processing
    }
}
