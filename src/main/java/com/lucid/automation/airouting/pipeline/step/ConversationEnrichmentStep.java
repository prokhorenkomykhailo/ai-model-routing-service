package com.lucid.automation.airouting.pipeline.step;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucid.automation.common.dto.enrichment.ConversationEnrichment;
import com.lucid.automation.common.dto.enrichment.TopicEnrichment;
import com.lucid.automation.common.dto.enrichment.UrgencyLevel;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.pipeline.context.PostProcessingContext;
import com.lucid.automation.airouting.util.json.JsonCleaner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

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
                TopicEnrichment topic = createBasicTopic(topicMap);
                topics.add(topic);
                logger.info("GEMINI-PARSE: Successfully parsed topic {}: '{}'", i, topic.title());
            }
        }
        return new ConversationEnrichment(topics, List.of(), List.of(), null);
    }
    
    private TopicEnrichment createBasicTopic(Map<?, ?> topicMap) {
        String title = extractStringValue(topicMap, "title", "Untitled Topic");
        String shortSummary = extractStringValue(topicMap, "shortSummary", DEFAULT_SUMMARY);
        String fullSummary = extractStringValue(topicMap, "fullSummary", DEFAULT_DETAILED_SUMMARY);
        String suggestedAction = extractStringValue(topicMap, "suggestedAction", DEFAULT_ACTION);
        String category = extractStringValue(topicMap, "category", DEFAULT_CATEGORY);
        
        return new TopicEnrichment(
            title,
            shortSummary,
            fullSummary,
            suggestedAction,
            null, // clientOrSupplier
            null, // deadline
            UrgencyLevel.LOW, // default urgency
            category,
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
    }
    
    private String extractStringValue(Map<?, ?> map, String key, String defaultValue) {
        Object value = map.get(key);
        return value instanceof String str ? str : defaultValue;
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
