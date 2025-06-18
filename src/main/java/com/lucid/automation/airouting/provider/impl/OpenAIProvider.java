package com.lucid.automation.airouting.provider.impl;

import com.lucid.automation.airouting.provider.AIProvider;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.SlackParticipant;
import com.lucid.automation.airouting.dto.CategoryResult;
import com.lucid.automation.airouting.dto.SummaryResult;
import com.lucid.automation.airouting.dto.SentimentResult;
import com.lucid.automation.airouting.dto.UrgencyLevel;
import com.lucid.automation.airouting.dto.MessageEnrichment;
import com.lucid.automation.airouting.dto.ParticipantInsight;
import com.lucid.automation.airouting.dto.ConversationEnrichment;
import com.lucid.automation.airouting.dto.TopicEnrichment;

import com.lucid.automation.airouting.exception.AIProviderConfigurationException;
import com.lucid.automation.airouting.exception.AIProviderApiException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.*;
import java.util.stream.Collectors;

@Component("openaiProvider")
public class OpenAIProvider implements AIProvider {
    
    private static final Logger logger = LoggerFactory.getLogger(OpenAIProvider.class);
    
    // Constants
    private static final String PROVIDER_ID = "openaiProvider";
    private static final String GENERAL_CATEGORY = "General";
    private static final String UNKNOWN_INTENT = "Unknown";
    private static final String NEUTRAL_SENTIMENT = "Neutral";
    private static final String PARTICIPANT_ROLE = "Participant";
    private static final String GENERAL_DISCUSSION = "General Discussion";
    private static final String SENTIMENT_KEY = "Sentiment";
    
    @Value("${ai.providers.openai.api-key:}")
    private String apiKey;
    
    @Value("${ai.providers.openai.endpoint:https://api.openai.com/v1}")
    private String apiEndpoint;
    
    @Value("${ai.providers.openai.model:gpt-3.5-turbo}")
    private String model;
    
    @Value("${ai.providers.openai.timeout:30000}")
    private int timeoutMs;
    
    private final WebClient webClient;
    private final ObjectMapper objectMapper;
    private double lastConfidence = 0.0;
    
    public OpenAIProvider(WebClient webClient, ObjectMapper objectMapper) {
        this.webClient = webClient;
        this.objectMapper = objectMapper;
    }
    
    @Override
    public CategoryResult categorize(String content) {
        try {
            logger.debug("Categorizing content with OpenAI: length={}", content.length());
            
            String prompt = buildCategorizationPrompt(content);
            String response = callOpenAIAPI(prompt);
            
            CategoryResult result = parseCategorizationResponse(response);
            this.lastConfidence = result.confidence();
            
            logger.debug("Categorization result: category={}, confidence={}", 
                        result.category(), result.confidence());
            
            return result;
            
        } catch (Exception e) {
            logger.error("Failed to categorize with OpenAI", e);
            this.lastConfidence = 0.0;
            return new CategoryResult("Uncategorized", 0.0);
        }
    }
    
    @Override
    public SummaryResult summarize(String content) {
        try {
            logger.debug("Summarizing content with OpenAI: length={}", content.length());
            
            String prompt = buildSummarizationPrompt(content);
            String response = callOpenAIAPI(prompt);
            
            return parseSummaryResponse(response);
            
        } catch (Exception e) {
            logger.error("Failed to summarize with OpenAI", e);
            return new SummaryResult("Summary unavailable", "Error occurred");
        }
    }
    
    @Override
    public ConversationEnrichment enrichConversation(List<SlackMessage> messages, 
                                                   List<SlackParticipant> participants, 
                                                   List<String> availableCategories) {
        try {
            logger.info("Enriching conversation with OpenAI: {} messages, {} participants, {} categories", 
                       messages.size(), participants != null ? participants.size() : 0,
                       availableCategories != null ? availableCategories.size() : 0);
            
            String conversationText = formatConversationForAnalysis(messages);
            String prompt = buildConversationEnrichmentPrompt(conversationText, participants);
            String response = callOpenAIAPI(prompt);
            
            return parseConversationEnrichmentResponse(response, messages, participants);
            
        } catch (Exception e) {
            logger.error("Failed to enrich conversation with OpenAI", e);
            return getDefaultConversationEnrichment();
        }
    }
    
    @Override
    public MessageEnrichment enrichMessage(String content, Map<String, Object> context) {
        try {
            String prompt = buildMessageEnrichmentPrompt(content, context);
            String response = callOpenAIAPI(prompt);
            return parseMessageEnrichmentResponse(response);
            
        } catch (Exception e) {
            logger.error("Failed to enrich message with OpenAI", e);
            return new MessageEnrichment(GENERAL_CATEGORY, 0.0, UNKNOWN_INTENT, List.of(), 0.0);
        }
    }
    
    @Override
    public ParticipantInsight analyzeParticipant(SlackParticipant participant, List<SlackMessage> messages) {
        try {
            List<SlackMessage> userMessages = messages.stream()
                .filter(msg -> participant.getId().equals(msg.getUserId()))
                .toList();
                
            String prompt = buildParticipantAnalysisPrompt(participant, userMessages);
            String response = callOpenAIAPI(prompt);
            return parseParticipantAnalysisResponse(response);
            
        } catch (Exception e) {
            logger.error("Failed to analyze participant with OpenAI", e);
            return new ParticipantInsight(PARTICIPANT_ROLE, 0.0, NEUTRAL_SENTIMENT, 0);
        }
    }
    
    @Override
    public UrgencyLevel assessUrgency(List<SlackMessage> messages) {
        try {
            String conversationText = formatConversationForAnalysis(messages);
            String prompt = buildUrgencyAssessmentPrompt(conversationText);
            String response = callOpenAIAPI(prompt);
            return parseUrgencyResponse(response);
            
        } catch (Exception e) {
            logger.error("Failed to assess urgency with OpenAI", e);
            return UrgencyLevel.LOW;
        }
    }
    
    @Override
    public String generateTopic(List<SlackMessage> messages) {
        try {
            String conversationText = formatConversationForAnalysis(messages.stream().limit(10).toList());
            String prompt = buildTopicGenerationPrompt(conversationText);
            String response = callOpenAIAPI(prompt);
            return parseTopicResponse(response);
            
        } catch (Exception e) {
            logger.error("Failed to generate topic with OpenAI", e);
            return GENERAL_DISCUSSION;
        }
    }
    
    @Override
    public List<String> extractEntities(String content) {
        try {
            String prompt = buildEntityExtractionPrompt(content);
            String response = callOpenAIAPI(prompt);
            return parseEntitiesResponse(response);
            
        } catch (Exception e) {
            logger.error("Failed to extract entities with OpenAI", e);
            return List.of();
        }
    }
    
    @Override
    public SentimentResult analyzeSentiment(String content) {
        try {
            String prompt = buildSentimentAnalysisPrompt(content);
            String response = callOpenAIAPI(prompt);
            return parseSentimentResponse(response);
            
        } catch (Exception e) {
            logger.error("Failed to analyze sentiment with OpenAI", e);
            return new SentimentResult(NEUTRAL_SENTIMENT, 0.0, 0.0);
        }
    }
    
    @Override
    public String getProviderId() {
        return PROVIDER_ID;
    }
    
    @Override
    public boolean isAvailable() {
        try {
            if (apiKey == null || apiKey.trim().isEmpty()) {
                logger.warn("OpenAI API key not configured");
                return false;
            }
            
            // Simple test call
            String testPrompt = "Hello";
            callOpenAIAPI(testPrompt);
            return true;
            
        } catch (Exception e) {
            logger.warn("OpenAI provider unavailable: {}", e.getMessage());
            return false;
        }
    }
    
    @Override
    public double getLastConfidence() {
        return lastConfidence;
    }
    
    // Private helper methods
    private String callOpenAIAPI(String prompt) {
        if (apiKey == null || apiKey.trim().isEmpty()) {
            throw new AIProviderConfigurationException("OpenAI API key not configured", PROVIDER_ID);
        }
        
        List<Map<String, Object>> messages = List.of(
            Map.of("role", "user", "content", prompt)
        );
        
        Map<String, Object> requestBody = Map.of(
            "model", model,
            "messages", messages,
            "temperature", 0.1,
            "max_tokens", 2048,
            "top_p", 0.8
        );
        
        try {
            String url = apiEndpoint + "/chat/completions";
            
            String response = webClient
                .post()
                .uri(url)
                .header("Authorization", "Bearer " + apiKey)
                .header("Content-Type", "application/json")
                .bodyValue(requestBody)
                .retrieve()
                .bodyToMono(String.class)
                .block();
            
            return extractTextFromOpenAIResponse(response);
            
        } catch (Exception e) {
            logger.error("Error calling OpenAI API", e);
            throw new AIProviderApiException("Failed to call OpenAI API: " + e.getMessage(), PROVIDER_ID, e);
        }
    }
    
    private String extractTextFromOpenAIResponse(String responseBody) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> responseMap = objectMapper.readValue(responseBody, Map.class);
            
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> choices = (List<Map<String, Object>>) responseMap.get("choices");
            
            if (choices != null && !choices.isEmpty()) {
                Map<String, Object> choice = choices.get(0);
                
                @SuppressWarnings("unchecked")
                Map<String, Object> message = (Map<String, Object>) choice.get("message");
                
                if (message != null) {
                    return (String) message.get("content");
                }
            }
            
            throw new AIProviderApiException("No content found in OpenAI response", PROVIDER_ID);
            
        } catch (Exception e) {
            logger.error("Failed to parse OpenAI response: {}", responseBody, e);
            throw new AIProviderApiException("Failed to parse OpenAI response", PROVIDER_ID, e);
        }
    }
    
    // Prompt building methods
    private String buildCategorizationPrompt(String content) {
        return String.format("""
            Categorize this message into one of these categories:
            - Customer Inquiry
            - Technical Support
            - Billing Issue
            - Feature Request
            - Complaint
            - General Discussion
            - Urgent Issue
            
            Message: "%s"
            
            Respond with only the category name followed by a confidence score (0.0-1.0).
            Format: "Category|confidence"
            Example: "Customer Inquiry|0.85"
            """, content);
    }
    
    private String buildSummarizationPrompt(String content) {
        return String.format("""
            Summarize the following content in 2-3 sentences. Focus on key points and main ideas.
            
            Content: "%s"
            
            Summary:
            """, content);
    }
    
    private String buildConversationEnrichmentPrompt(String conversationText, List<SlackParticipant> participants) {
        String participantInfo = participants != null ? 
            participants.stream()
                .map(p -> String.format("- %s (%s)", p.getUsername(), p.getId()))
                .collect(Collectors.joining("\n")) : "No participant info";
        
        return String.format("""
            Analyze this conversation and provide enrichment data.
            
            Participants:
            %s
            
            Conversation:
            %s
            
            Provide analysis in this format:
            Topic: [main topic]
            Summary: [brief summary]
            Urgency: [LOW|MEDIUM|HIGH|CRITICAL]
            """, participantInfo, conversationText);
    }
    
    private String buildMessageEnrichmentPrompt(String content, Map<String, Object> context) {
        String contextStr = context != null ? context.toString() : "No context";
        return String.format("""
            Analyze this message and provide enrichment data.
            
            Message: "%s"
            Context: %s
            
            Provide analysis in this format:
            Category: [category]
            Sentiment: [score between -1 and 1]
            Intent: [intent]
            Entities: [comma-separated list]
            Confidence: [0.0-1.0]
            """, content, contextStr);
    }
    
    private String buildParticipantAnalysisPrompt(SlackParticipant participant, List<SlackMessage> messages) {
        String messagesText = messages.stream()
            .map(msg -> String.format("[%s] %s", msg.getTimestamp(), msg.getContent()))
            .collect(Collectors.joining("\n"));
        
        return String.format("""
            Analyze this participant's behavior and engagement.
            
            Participant: %s (%s)
            Messages (%d total):
            %s
            
            Provide analysis in this format:
            Role: [role/persona]
            Engagement: [score 0.0-1.0]
            Sentiment: [Positive|Negative|Neutral]
            MessageCount: [number]
            """, participant.getUsername(), participant.getId(), messages.size(), messagesText);
    }
    
    private String buildUrgencyAssessmentPrompt(String conversationText) {
        return String.format("""
            Assess the urgency level of this conversation.
            
            Conversation:
            %s
            
            Respond with only one of: LOW, MEDIUM, HIGH, CRITICAL
            """, conversationText);
    }
    
    private String buildTopicGenerationPrompt(String conversationText) {
        return String.format("""
            Generate a concise topic title for this conversation (max 5 words).
            
            Conversation:
            %s
            
            Topic:
            """, conversationText);
    }
    
    private String buildEntityExtractionPrompt(String content) {
        return String.format("""
            Extract named entities from this text. Include people, organizations, locations, dates, etc.
            
            Text: "%s"
            
            Return entities as a comma-separated list. If no entities, return "none".
            """, content);
    }
    
    private String buildSentimentAnalysisPrompt(String content) {
        return String.format("""
            Analyze the sentiment of this text.
            
            Text: "%s"
            
            Respond in format:
            Sentiment: [Positive|Negative|Neutral]
            Score: [numeric score between -1 and 1]
            Confidence: [confidence between 0 and 1]
            """, content);
    }
    
    // Response parsing methods
    private CategoryResult parseCategorizationResponse(String response) {
        try {
            String[] parts = response.trim().split("\\|");
            if (parts.length >= 2) {
                String category = parts[0].trim();
                double confidence = Double.parseDouble(parts[1].trim());
                return new CategoryResult(category, confidence);
            } else {
                return new CategoryResult(response.trim(), 0.8);
            }
        } catch (Exception e) {
            logger.warn("Failed to parse categorization response: {}", response, e);
            return new CategoryResult(GENERAL_DISCUSSION, 0.5);
        }
    }
    
    private SummaryResult parseSummaryResponse(String response) {
        return new SummaryResult(response.trim(), "Generated by OpenAI");
    }
    
    private ConversationEnrichment parseConversationEnrichmentResponse(String response, 
            List<SlackMessage> messages, List<SlackParticipant> participants) {
        try {
            Map<String, String> parsed = parseStructuredResponse(response);
            
            String topic = parsed.getOrDefault("Topic", GENERAL_DISCUSSION);
            String summary = parsed.getOrDefault("Summary", "No summary available");
            UrgencyLevel urgency = parseUrgencyLevel(parsed.getOrDefault("Urgency", "LOW"));
            
            // Create participant insights
            List<ParticipantInsight> participantInsights = participants != null ? 
                participants.stream()
                    .map(p -> new ParticipantInsight(PARTICIPANT_ROLE, 0.5, NEUTRAL_SENTIMENT, 
                        (int) messages.stream().filter(m -> p.getId().equals(m.getUserId())).count()))
                    .toList() : List.of();
            
            // Create message enrichments
            List<MessageEnrichment> messageEnrichments = messages.stream()
                .map(m -> new MessageEnrichment(GENERAL_CATEGORY, 0.0, UNKNOWN_INTENT, List.of(), 0.7))
                .toList();
            
            TopicEnrichment topicEnrichment = new TopicEnrichment(
                topic, summary, summary, "No action suggested", null, null,
                urgency, GENERAL_CATEGORY, null, null, List.of(), Map.of(), List.of(), null, null
            );
            
            return new ConversationEnrichment(List.of(topicEnrichment), 
                participantInsights, messageEnrichments, Map.of(PROVIDER_ID, PROVIDER_ID));
            
        } catch (Exception e) {
            logger.error("Failed to parse conversation enrichment response", e);
            return getDefaultConversationEnrichment();
        }
    }
    
    private MessageEnrichment parseMessageEnrichmentResponse(String response) {
        try {
            Map<String, String> parsed = parseStructuredResponse(response);
            
            String category = parsed.getOrDefault("Category", GENERAL_CATEGORY);
            double sentiment = Double.parseDouble(parsed.getOrDefault(SENTIMENT_KEY, "0.0"));
            String intent = parsed.getOrDefault("Intent", UNKNOWN_INTENT);
            List<String> entities = Arrays.asList(parsed.getOrDefault("Entities", "").split(","))
                .stream().map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());
            double confidence = Double.parseDouble(parsed.getOrDefault("Confidence", "0.7"));
            
            return new MessageEnrichment(category, sentiment, intent, entities, confidence);
            
        } catch (Exception e) {
            logger.error("Failed to parse message enrichment response", e);
            return new MessageEnrichment(GENERAL_CATEGORY, 0.0, UNKNOWN_INTENT, List.of(), 0.5);
        }
    }
    
    private ParticipantInsight parseParticipantAnalysisResponse(String response) {
        try {
            Map<String, String> parsed = parseStructuredResponse(response);
            
            String role = parsed.getOrDefault("Role", PARTICIPANT_ROLE);
            double engagement = Double.parseDouble(parsed.getOrDefault("Engagement", "0.5"));
            String sentiment = parsed.getOrDefault(SENTIMENT_KEY, NEUTRAL_SENTIMENT);
            int messageCount = Integer.parseInt(parsed.getOrDefault("MessageCount", "0"));
            
            return new ParticipantInsight(role, engagement, sentiment, messageCount);
            
        } catch (Exception e) {
            logger.error("Failed to parse participant analysis response", e);
            return new ParticipantInsight(PARTICIPANT_ROLE, 0.5, NEUTRAL_SENTIMENT, 0);
        }
    }
    
    private UrgencyLevel parseUrgencyResponse(String response) {
        String urgency = response.trim().toUpperCase();
        try {
            return UrgencyLevel.valueOf(urgency);
        } catch (IllegalArgumentException e) {
            logger.warn("Unknown urgency level: {}", urgency);
            return UrgencyLevel.LOW;
        }
    }
    
    private String parseTopicResponse(String response) {
        return response.trim().replace("Topic:", "").trim();
    }
    
    private List<String> parseEntitiesResponse(String response) {
        if (response.trim().equalsIgnoreCase("none") || response.trim().isEmpty()) {
            return List.of();
        }
        return Arrays.asList(response.split(","))
            .stream()
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toList());
    }
    
    private SentimentResult parseSentimentResponse(String response) {
        try {
            Map<String, String> parsed = parseStructuredResponse(response);
            
            String sentiment = parsed.getOrDefault(SENTIMENT_KEY, NEUTRAL_SENTIMENT);
            double score = Double.parseDouble(parsed.getOrDefault("Score", "0.0"));
            double confidence = Double.parseDouble(parsed.getOrDefault("Confidence", "0.7"));
            
            return new SentimentResult(sentiment, score, confidence);
            
        } catch (Exception e) {
            logger.error("Failed to parse sentiment response", e);
            return new SentimentResult(NEUTRAL_SENTIMENT, 0.0, 0.5);
        }
    }
    
    // Utility methods
    private Map<String, String> parseStructuredResponse(String response) {
        Map<String, String> result = new HashMap<>();
        String[] lines = response.split("\n");
        
        for (String line : lines) {
            if (line.contains(":")) {
                String[] parts = line.split(":", 2);
                if (parts.length == 2) {
                    result.put(parts[0].trim(), parts[1].trim());
                }
            }
        }
        
        return result;
    }
    
    private UrgencyLevel parseUrgencyLevel(String urgency) {
        try {
            return UrgencyLevel.valueOf(urgency.toUpperCase());
        } catch (IllegalArgumentException e) {
            return UrgencyLevel.LOW;
        }
    }
    
    private String formatConversationForAnalysis(List<SlackMessage> messages) {
        return messages.stream()
            .map(msg -> String.format("[%s] %s: %s", 
                msg.getTimestamp(), msg.getUserId(), msg.getContent()))
            .collect(Collectors.joining("\n"));
    }
    
    private ConversationEnrichment getDefaultConversationEnrichment() {
        TopicEnrichment defaultTopic = new TopicEnrichment(
            GENERAL_DISCUSSION,
            "No summary available",
            "No detailed summary available",
            "No action suggested",
            null,
            null,
            UrgencyLevel.LOW,
            GENERAL_CATEGORY,
            null, // startTime
            null, // endTime
            List.of(),
            Map.of(),
            List.of(),
            null,
            null
        );
        
        return new ConversationEnrichment(
            List.of(defaultTopic),
            List.of(),
            List.of(),
            Map.of("provider", "openai", "error", "true")
        );
    }
}