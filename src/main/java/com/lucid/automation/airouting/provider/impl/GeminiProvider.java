package com.lucid.automation.airouting.provider.impl;

import com.lucid.automation.airouting.provider.AIProvider;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.SlackParticipant;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.genai.Client;
import com.google.genai.types.GenerateContentResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Component("geminiProvider")
public class GeminiProvider implements AIProvider {
    
    private static final Logger logger = LoggerFactory.getLogger(GeminiProvider.class);
    
    @Value("${ai.providers.gemini.api-key:}")
    private String apiKey;
    
    @Value("${ai.providers.gemini.endpoint:https://generativelanguage.googleapis.com/v1/models}")
    private String apiEndpoint;
    
    @Value("${ai.providers.gemini.model:gemini-2.0-flash}")
    private String model;
    
    private final Client geminiClient;
    private final ObjectMapper objectMapper;
    private double lastConfidence = 0.0;
    
    public GeminiProvider(ObjectMapper objectMapper) {
        // The client gets the API key from the environment variable `GOOGLE_API_KEY`
        this.geminiClient = new Client();
        this.objectMapper = objectMapper;
    }
    
    @Override
    public CategoryResult categorize(String content) {
        try {
            logger.debug("Categorizing content with Gemini: length={}", content.length());
            
            String prompt = buildCategorizationPrompt(content);
            String response = callGeminiAPI(prompt);
            
            CategoryResult result = parseCategorizationResponse(response);
            this.lastConfidence = result.confidence();
            
            logger.debug("Categorization result: category={}, confidence={}", 
                        result.category(), result.confidence());
            
            return result;
            
        } catch (Exception e) {
            logger.error("Failed to categorize with Gemini", e);
            this.lastConfidence = 0.0;
            return new CategoryResult("Uncategorized", 0.0);
        }
    }
    
    @Override
    public SummaryResult summarize(String content) {
        try {
            logger.debug("Summarizing content with Gemini: length={}", content.length());
            
            String prompt = buildSummarizationPrompt(content);
            String response = callGeminiAPI(prompt);
            
            return parseSummaryResponse(response);
            
        } catch (Exception e) {
            logger.error("Failed to summarize with Gemini", e);
            return new SummaryResult("Summary unavailable", "Error occurred");
        }
    }
    
    @Override
    public ConversationEnrichment enrichConversation(List<SlackMessage> messages, List<SlackParticipant> participants) {
        try {
            logger.info("Enriching conversation with Gemini: {} messages, {} participants", 
                       messages.size(), participants != null ? participants.size() : 0);
            
            String conversationText = formatConversationForAnalysis(messages);
            String prompt = buildConversationEnrichmentPrompt(conversationText, participants);
            String response = callGeminiAPI(prompt);
            
            return parseConversationEnrichmentResponse(response, messages, participants);
            
        } catch (Exception e) {
            logger.error("Failed to enrich conversation with Gemini", e);
            return getDefaultConversationEnrichment();
        }
    }
    
    @Override
    public MessageEnrichment enrichMessage(String content, Map<String, Object> context) {
        try {
            String prompt = buildMessageEnrichmentPrompt(content, context);
            String response = callGeminiAPI(prompt);
            return parseMessageEnrichmentResponse(response);
            
        } catch (Exception e) {
            logger.error("Failed to enrich message with Gemini", e);
            return new MessageEnrichment("General", 0.0, "Unknown", List.of(), 0.0);
        }
    }
    
    @Override
    public ParticipantInsight analyzeParticipant(SlackParticipant participant, List<SlackMessage> messages) {
        try {
            List<SlackMessage> userMessages = messages.stream()
                .filter(msg -> participant.getId().equals(msg.getUserId()))
                .collect(Collectors.toList());
                
            String prompt = buildParticipantAnalysisPrompt(participant, userMessages);
            String response = callGeminiAPI(prompt);
            return parseParticipantAnalysisResponse(response);
            
        } catch (Exception e) {
            logger.error("Failed to analyze participant with Gemini", e);
            return new ParticipantInsight("Participant", 0.0, "Neutral", 0);
        }
    }
    
    @Override
    public UrgencyLevel assessUrgency(List<SlackMessage> messages) {
        try {
            String conversationText = formatConversationForAnalysis(messages);
            String prompt = buildUrgencyAssessmentPrompt(conversationText);
            String response = callGeminiAPI(prompt);
            return parseUrgencyResponse(response);
            
        } catch (Exception e) {
            logger.error("Failed to assess urgency with Gemini", e);
            return UrgencyLevel.LOW;
        }
    }
    
    @Override
    public String generateTopic(List<SlackMessage> messages) {
        try {
            String conversationText = formatConversationForAnalysis(messages.stream().limit(10).collect(Collectors.toList()));
            String prompt = buildTopicGenerationPrompt(conversationText);
            String response = callGeminiAPI(prompt);
            return parseTopicResponse(response);
            
        } catch (Exception e) {
            logger.error("Failed to generate topic with Gemini", e);
            return "General Discussion";
        }
    }
    
    @Override
    public List<String> extractEntities(String content) {
        try {
            String prompt = buildEntityExtractionPrompt(content);
            String response = callGeminiAPI(prompt);
            return parseEntitiesResponse(response);
            
        } catch (Exception e) {
            logger.error("Failed to extract entities with Gemini", e);
            return List.of();
        }
    }
    
    @Override
    public SentimentResult analyzeSentiment(String content) {
        try {
            String prompt = buildSentimentAnalysisPrompt(content);
            String response = callGeminiAPI(prompt);
            return parseSentimentResponse(response);
            
        } catch (Exception e) {
            logger.error("Failed to analyze sentiment with Gemini", e);
            return new SentimentResult("Neutral", 0.0, 0.0);
        }
    }
    
    @Override
    public String getProviderId() {
        return "gemini";
    }
    
    @Override
    public boolean isAvailable() {
        try {
            // Check if GOOGLE_API_KEY environment variable is set
            String googleApiKey = System.getenv("GOOGLE_API_KEY");
            if (googleApiKey == null || googleApiKey.trim().isEmpty()) {
                logger.warn("Gemini provider unavailable: GOOGLE_API_KEY environment variable not set");
                return false;
            }
            
            // Simple test call to verify API availability
            String testPrompt = "Hello";
            callGeminiAPI(testPrompt);
            return true;
            
        } catch (Exception e) {
            logger.warn("Gemini provider unavailable: {}", e.getMessage());
            return false;
        }
    }
    
    @Override
    public double getLastConfidence() {
        return lastConfidence;
    }
    
    // Private helper methods
    private String callGeminiAPI(String prompt) {
        try {
            logger.debug("Calling Gemini API with prompt length: {}", prompt.length());
            
            GenerateContentResponse response = geminiClient.models.generateContent(
                model, 
                prompt, 
                null
            );
            
            String responseText = response.text();
            logger.debug("Received response from Gemini API");
            
            return responseText;
            
        } catch (Exception e) {
            logger.error("Error calling Gemini API", e);
            throw new RuntimeException("Failed to call Gemini API: " + e.getMessage(), e);
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
        StringBuilder participantInfo = new StringBuilder();
        List<String> participantNames = new ArrayList<>();
        
        if (participants != null) {
            participantInfo.append("Participants: ");
            participants.forEach(p -> {
                participantInfo.append(p.getUsername()).append(" ");
                participantNames.add(p.getUsername());
            });
        }
        
        return String.format("""
            Analyze this Slack conversation and provide comprehensive enrichment.
            %s
            
            Conversation:
            %s
            
            Provide analysis in JSON format with these exact fields:
            {
                "topic": {
                    "name": "concise topic name",
                    "summary": "detailed summary of the conversation",
                    "category": ["primary category", "secondary category"],
                    "sub-category": ["specific sub-category 1", "specific sub-category 2"],
                    "keyPoints": [
                        "most important point 1",
                        "most important point 2",
                        "most important point 3"
                    ],
                    "priority": "Low|Medium|High|Critical",
                    "keywords": [
                        "relevant keyword 1",
                        "relevant keyword 2",
                        "relevant keyword 3"
                    ]
                },
                "conversations": [
                    {
                        "text": "actual message content from conversation",
                        "relevance": "explanation of why this message is important to the topic"
                    }
                ],
                "peopleInvolved": %s
            }
            
            Instructions:
            - For topic.category: Use broad categories like "Technical", "Business", "Support", "Planning", etc.
            - For topic.sub-category: Use specific categories like "Bug Fix", "Feature Request", "Payment Issues", etc.
            - For conversations: Include 2-5 most relevant messages that capture the essence of the discussion
            - For topic.priority: Base on urgency, impact, and time sensitivity
            - For topic.keywords: Extract 3-7 key terms that best represent the conversation content
            """, 
            participantInfo.toString(), 
            conversationText,
            participantNames.isEmpty() ? "[]" : participantNames.toString());
    }
    
    private String buildMessageEnrichmentPrompt(String content, Map<String, Object> context) {
        return String.format("""
            Analyze this message and provide enrichment data:
            
            Message: "%s"
            Context: %s
            
            Provide response in format:
            Category: [category]
            Sentiment: [sentiment_score between -1 and 1]
            Intent: [user intent]
            Entities: [comma-separated entities]
            Confidence: [confidence_score between 0 and 1]
            """, content, context != null ? context.toString() : "None");
    }
    
    private String buildParticipantAnalysisPrompt(SlackParticipant participant, List<SlackMessage> messages) {
        String messagesText = messages.stream()
            .map(SlackMessage::getContent)
            .collect(Collectors.joining("\n"));
            
        return String.format("""
            Analyze this participant's communication patterns:
            
            Participant: %s
            Messages (%d total):
            %s
            
            Provide analysis in format:
            Role: [participant role in conversation]
            Engagement: [engagement level 0.0-1.0]
            Sentiment: [dominant sentiment]
            """, participant.getUsername(), messages.size(), messagesText);
    }
    
    private String buildUrgencyAssessmentPrompt(String conversationText) {
        return String.format("""
            Assess the urgency level of this conversation.
            Consider factors like: time sensitivity, impact level, escalation words, customer satisfaction.
            
            Conversation:
            %s
            
            Respond with only one of: LOW, MEDIUM, HIGH, CRITICAL
            """, conversationText);
    }
    
    private String buildTopicGenerationPrompt(String conversationText) {
        return String.format("""
            Generate a concise topic/title for this conversation (max 5 words):
            
            Conversation:
            %s
            
            Topic:
            """, conversationText);
    }
    
    private String buildEntityExtractionPrompt(String content) {
        return String.format("""
            Extract key entities from this text. Include names, organizations, products, locations, etc.
            
            Text: "%s"
            
            Respond with comma-separated entities:
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
            return new CategoryResult("General Discussion", 0.5);
        }
    }
    
    private SummaryResult parseSummaryResponse(String response) {
        return new SummaryResult(response.trim(), "Generated by Gemini");
    }
    
    private ConversationEnrichment parseConversationEnrichmentResponse(String response, 
            List<SlackMessage> messages, List<SlackParticipant> participants) {
        
        try {
            String cleanedResponse = cleanJsonResponse(response);
            @SuppressWarnings("unchecked")
            Map<String, Object> analysis = objectMapper.readValue(cleanedResponse, Map.class);
            
            TopicInfo topicInfo = extractTopicInfo(analysis);
            
            List<ParticipantInsight> participantInsights = participants != null ? 
                participants.stream()
                    .map(p -> analyzeParticipant(p, messages))
                    .toList() : 
                List.of();
            
            List<MessageEnrichment> messageEnrichments = messages.stream()
                .map(msg -> enrichMessage(msg.getContent(), Map.of()))
                .toList();
            
            return new ConversationEnrichment(topicInfo.topic(), topicInfo.summary(), topicInfo.urgency(), 
                                            participantInsights, messageEnrichments, analysis);
                                            
        } catch (Exception e) {
            logger.warn("Failed to parse conversation enrichment response, using fallback", e);
            return getDefaultConversationEnrichment();
        }
    }
    
    private TopicInfo extractTopicInfo(Map<String, Object> analysis) {
        String topic = "General Discussion";
        String summary = "No summary available";
        UrgencyLevel urgency = UrgencyLevel.LOW;
        
        if (analysis.containsKey("topic") && analysis.get("topic") instanceof Map<?, ?> topicMap) {
            topic = extractStringValue(topicMap, "name", topic);
            summary = extractStringValue(topicMap, "summary", summary);
            urgency = extractUrgencyFromPriority(topicMap);
        } else {
            // Fallback to old format
            topic = extractStringValue(analysis, "topic", topic);
            summary = extractStringValue(analysis, "summary", summary);
            urgency = extractUrgencyFromField(analysis);
        }
        
        return new TopicInfo(topic, summary, urgency);
    }
    
    private String extractStringValue(Map<?, ?> map, String key, String defaultValue) {
        Object value = map.get(key);
        return value instanceof String str ? str : defaultValue;
    }
    
    private UrgencyLevel extractUrgencyFromPriority(Map<?, ?> topicMap) {
        String priorityStr = extractStringValue(topicMap, "priority", "LOW").toUpperCase();
        return mapPriorityToUrgency(priorityStr);
    }
    
    private UrgencyLevel extractUrgencyFromField(Map<?, ?> analysis) {
        String urgencyStr = extractStringValue(analysis, "urgency", "LOW").toUpperCase();
        try {
            return UrgencyLevel.valueOf(urgencyStr);
        } catch (Exception e) {
            logger.warn("Failed to parse urgency '{}', using LOW", urgencyStr);
            return UrgencyLevel.LOW;
        }
    }
    
    private UrgencyLevel mapPriorityToUrgency(String priorityStr) {
        return switch (priorityStr) {
            case "CRITICAL" -> UrgencyLevel.CRITICAL;
            case "HIGH" -> UrgencyLevel.HIGH;
            case "MEDIUM" -> UrgencyLevel.MEDIUM;
            default -> UrgencyLevel.LOW;
        };
    }
    
    private record TopicInfo(String topic, String summary, UrgencyLevel urgency) {}
    
    private MessageEnrichment parseMessageEnrichmentResponse(String response) {
        try {
            String[] lines = response.split("\n");
            String category = "General";
            double sentiment = 0.0;
            String intent = "Unknown";
            List<String> entities = List.of();
            double confidence = 0.5;
            
            for (String line : lines) {
                if (line.startsWith("Category:")) {
                    category = line.substring(9).trim();
                } else if (line.startsWith("Sentiment:")) {
                    sentiment = Double.parseDouble(line.substring(10).trim());
                } else if (line.startsWith("Intent:")) {
                    intent = line.substring(7).trim();
                } else if (line.startsWith("Entities:")) {
                    String entitiesStr = line.substring(9).trim();
                    entities = Arrays.asList(entitiesStr.split(",\\s*"));
                } else if (line.startsWith("Confidence:")) {
                    confidence = Double.parseDouble(line.substring(11).trim());
                }
            }
            
            return new MessageEnrichment(category, sentiment, intent, entities, confidence);
            
        } catch (Exception e) {
            logger.warn("Failed to parse message enrichment response: {}", response, e);
            return new MessageEnrichment("General", 0.0, "Unknown", List.of(), 0.0);
        }
    }
    
    private ParticipantInsight parseParticipantAnalysisResponse(String response) {
        try {
            String[] lines = response.split("\n");
            String role = "Participant";
            double engagement = 0.5;
            String sentiment = "Neutral";
            
            for (String line : lines) {
                if (line.startsWith("Role:")) {
                    role = line.substring(5).trim();
                } else if (line.startsWith("Engagement:")) {
                    engagement = Double.parseDouble(line.substring(11).trim());
                } else if (line.startsWith("Sentiment:")) {
                    sentiment = line.substring(10).trim();
                }
            }
            
            return new ParticipantInsight(role, engagement, sentiment, 0);
            
        } catch (Exception e) {
            logger.warn("Failed to parse participant analysis response: {}", response, e);
            return new ParticipantInsight("Participant", 0.5, "Neutral", 0);
        }
    }
    
    private UrgencyLevel parseUrgencyResponse(String response) {
        try {
            String urgencyStr = response.trim().toUpperCase();
            return UrgencyLevel.valueOf(urgencyStr);
        } catch (Exception e) {
            logger.warn("Failed to parse urgency response: {}", response, e);
            return UrgencyLevel.LOW;
        }
    }
    
    private String parseTopicResponse(String response) {
        return response.trim();
    }
    
    private List<String> parseEntitiesResponse(String response) {
        try {
            return Arrays.asList(response.trim().split(",\\s*"));
        } catch (Exception e) {
            logger.warn("Failed to parse entities response: {}", response, e);
            return List.of();
        }
    }
    
    private SentimentResult parseSentimentResponse(String response) {
        try {
            String[] lines = response.split("\n");
            String sentiment = "Neutral";
            double score = 0.0;
            double confidence = 0.5;
            
            for (String line : lines) {
                if (line.startsWith("Sentiment:")) {
                    sentiment = line.substring(10).trim();
                } else if (line.startsWith("Score:")) {
                    score = Double.parseDouble(line.substring(6).trim());
                } else if (line.startsWith("Confidence:")) {
                    confidence = Double.parseDouble(line.substring(11).trim());
                }
            }
            
            return new SentimentResult(sentiment, score, confidence);
            
        } catch (Exception e) {
            logger.warn("Failed to parse sentiment response: {}", response, e);
            return new SentimentResult("Neutral", 0.0, 0.5);
        }
    }
    
    // Utility methods
    private String formatConversationForAnalysis(List<SlackMessage> messages) {
        return messages.stream()
            .map(msg -> String.format("[%s] %s: %s", 
                msg.getTimestamp(), msg.getUsername(), msg.getContent()))
            .collect(Collectors.joining("\n"));
    }
    
    private ConversationEnrichment getDefaultConversationEnrichment() {
        return new ConversationEnrichment(
            "General Discussion",
            "No summary available",
            UrgencyLevel.LOW,
            List.of(),
            List.of(),
            Map.of("error", "Failed to analyze conversation")
        );
    }
    
    /**
     * Cleans JSON response by removing markdown code block formatting if present.
     * Handles responses that start with ```json or ``` and end with ```
     */
    private String cleanJsonResponse(String response) {
        if (response == null || response.trim().isEmpty()) {
            return response;
        }
        
        String trimmed = response.trim();
        
        // Check if response is wrapped in markdown code blocks
        if (trimmed.startsWith("```")) {
            // Find the first newline after the opening ```
            int firstNewline = trimmed.indexOf('\n');
            if (firstNewline > 0) {
                // Remove the opening ``` line
                trimmed = trimmed.substring(firstNewline + 1);
            }
            
            // Remove closing ``` if present
            if (trimmed.endsWith("```")) {
                trimmed = trimmed.substring(0, trimmed.length() - 3).trim();
            }
        }
        
        return trimmed.trim();
    }
}
