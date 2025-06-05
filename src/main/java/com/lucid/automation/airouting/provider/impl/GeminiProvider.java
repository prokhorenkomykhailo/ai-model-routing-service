package com.lucid.automation.airouting.provider.impl;

import com.lucid.automation.airouting.provider.AIProvider;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.SlackParticipant;
import com.lucid.automation.airouting.client.DataStorageServiceClient;
import com.lucid.automation.airouting.dto.CategoryDTO;
import com.lucid.automation.airouting.dto.APIResponse;
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
    
    @Value("${app.tenant.default-id:default}")
    private String defaultTenantId;
    
    @Value("${app.tenant.default-schema:public}")
    private String defaultTenantSchema;
    
    private final Client geminiClient;
    private final ObjectMapper objectMapper;
    private final DataStorageServiceClient dataStorageServiceClient;
    private double lastConfidence = 0.0;
    private final boolean isClientAvailable;
    
    public GeminiProvider(ObjectMapper objectMapper, DataStorageServiceClient dataStorageServiceClient) {
        this.objectMapper = objectMapper;
        this.dataStorageServiceClient = dataStorageServiceClient;
        
        // Try to initialize the client, but handle gracefully if API key is not available
        Client tempClient = null;
        boolean clientAvailable = false;
        
        try {
            // Check if GOOGLE_API_KEY environment variable is set before initializing client
            String googleApiKey = System.getenv("GOOGLE_API_KEY");
            if (googleApiKey != null && !googleApiKey.trim().isEmpty()) {
                tempClient = new Client();
                clientAvailable = true;
                logger.info("Gemini client initialized successfully");
            } else {
                logger.warn("GOOGLE_API_KEY not set, Gemini provider will be unavailable");
            }
        } catch (Exception e) {
            logger.warn("Failed to initialize Gemini client: {}", e.getMessage());
        }
        
        this.geminiClient = tempClient;
        this.isClientAvailable = clientAvailable;
    }
    
    @Override
    public CategoryResult categorize(String content) {
        String debugId = "CATEGORIZE-" + System.currentTimeMillis();
        try {
            logger.debug("GEMINI-DEBUG [{}]: Starting categorization - content length: {}", debugId, content != null ? content.length() : 0);
            
            // Input validation
            if (content == null || content.trim().isEmpty()) {
                logger.warn("GEMINI-DEBUG [{}]: Empty or null content provided for categorization", debugId);
                this.lastConfidence = 0.0;
                return new CategoryResult("Uncategorized", 0.0);
            }
            
            if (!isClientAvailable) {
                logger.warn("GEMINI-DEBUG [{}]: Client not available for categorization", debugId);
                this.lastConfidence = 0.0;
                return new CategoryResult("Uncategorized", 0.0);
            }
            
            String prompt = buildCategorizationPrompt(content);
            logger.debug("GEMINI-DEBUG [{}]: Built categorization prompt, length: {}", debugId, prompt.length());
            
            String response = callGeminiAPI(prompt);
            logger.debug("GEMINI-DEBUG [{}]: Received API response for categorization", debugId);
            
            CategoryResult result = parseCategorizationResponse(response);
            this.lastConfidence = result.confidence();
            
            logger.debug("GEMINI-DEBUG [{}]: Categorization successful - category: {}, confidence: {}", 
                        debugId, result.category(), result.confidence());
            
            return result;
            
        } catch (IllegalArgumentException e) {
            logger.error("GEMINI-DEBUG [{}]: Invalid input for categorization: {}", debugId, e.getMessage(), e);
            this.lastConfidence = 0.0;
            return new CategoryResult("Uncategorized", 0.0);
        } catch (RuntimeException e) {
            logger.error("GEMINI-DEBUG [{}]: API error during categorization: {}", debugId, e.getMessage(), e);
            this.lastConfidence = 0.0;
            return new CategoryResult("Uncategorized", 0.0);
        } catch (Exception e) {
            logger.error("GEMINI-DEBUG [{}]: Unexpected error during categorization: {}", debugId, e.getMessage(), e);
            this.lastConfidence = 0.0;
            return new CategoryResult("Uncategorized", 0.0);
        }
    }
    
    @Override
    public SummaryResult summarize(String content) {
        String debugId = "SUMMARIZE-" + System.currentTimeMillis();
        try {
            logger.debug("GEMINI-DEBUG [{}]: Starting summarization - content length: {}", debugId, content != null ? content.length() : 0);
            
            // Input validation
            if (content == null || content.trim().isEmpty()) {
                logger.warn("GEMINI-DEBUG [{}]: Empty or null content provided for summarization", debugId);
                return new SummaryResult("Summary unavailable", "No content provided");
            }
            
            if (!isClientAvailable) {
                logger.warn("GEMINI-DEBUG [{}]: Client not available for summarization", debugId);
                return new SummaryResult("Summary unavailable", "AI service unavailable");
            }
            
            String prompt = buildSummarizationPrompt(content);
            logger.debug("GEMINI-DEBUG [{}]: Built summarization prompt, length: {}", debugId, prompt.length());
            
            String response = callGeminiAPI(prompt);
            logger.debug("GEMINI-DEBUG [{}]: Received API response for summarization", debugId);
            
            SummaryResult result = parseSummaryResponse(response);
            logger.debug("GEMINI-DEBUG [{}]: Summarization successful", debugId);
            
            return result;
            
        } catch (IllegalArgumentException e) {
            logger.error("GEMINI-DEBUG [{}]: Invalid input for summarization: {}", debugId, e.getMessage(), e);
            return new SummaryResult("Summary unavailable", "Invalid input: " + e.getMessage());
        } catch (RuntimeException e) {
            logger.error("GEMINI-DEBUG [{}]: API error during summarization: {}", debugId, e.getMessage(), e);
            return new SummaryResult("Summary unavailable", "Service error occurred");
        } catch (Exception e) {
            logger.error("GEMINI-DEBUG [{}]: Unexpected error during summarization: {}", debugId, e.getMessage(), e);
            return new SummaryResult("Summary unavailable", "Error occurred");
        }
    }
    
    /**
     * Enhanced enrichConversation method that accepts available categories as input parameter
     * 
     * @param messages The list of Slack messages to analyze
     * @param participants The list of participants in the conversation
     * @param availableCategories The list of available categories to use for categorization.
     *                           If null or empty, will attempt to fetch from data storage service,
     *                           and fallback to default categories if that fails.
     * @return ConversationEnrichment object containing the analysis results
     */
    @Override
    public ConversationEnrichment enrichConversation(List<SlackMessage> messages, 
                                                   List<SlackParticipant> participants, 
                                                   List<String> availableCategories) {
        String debugId = "ENRICH-CONV-" + System.currentTimeMillis();
        try {
            int messagesCount = messages != null ? messages.size() : 0;
            int participantsCount = participants != null ? participants.size() : 0;
            int categoriesCount = availableCategories != null ? availableCategories.size() : 0;
            logger.info("GEMINI-DEBUG [{}]: Starting conversation enrichment - {} messages, {} participants, {} categories provided", 
                       debugId, messagesCount, participantsCount, categoriesCount);
            
            // Input validation
            if (messages == null || messages.isEmpty()) {
                logger.warn("GEMINI-DEBUG [{}]: No messages provided for conversation enrichment", debugId);
                return getDefaultConversationEnrichment();
            }
            
            if (!isClientAvailable) {
                logger.warn("GEMINI-DEBUG [{}]: Client not available for conversation enrichment", debugId);
                return getDefaultConversationEnrichment();
            }
            
            String conversationText = formatConversationForAnalysis(messages);
            logger.debug("GEMINI-DEBUG [{}]: Formatted conversation text, length: {}", debugId, conversationText.length());
            
            // Use provided categories or fetch from data storage service if none provided
            List<String> categoriesToUse = (availableCategories != null && !availableCategories.isEmpty()) 
                ? availableCategories 
                : fetchAvailableCategories(debugId);
            
            String prompt = buildConversationEnrichmentPrompt(conversationText, participants, categoriesToUse);
            logger.debug("GEMINI-DEBUG [{}]: Built enrichment prompt, length: {}", debugId, prompt.length());
            
            String response = callGeminiAPI(prompt);
            logger.debug("GEMINI-DEBUG [{}]: Received API response for conversation enrichment", debugId);
            
            ConversationEnrichment result = parseConversationEnrichmentResponse(response, messages, participants);
            logger.info("GEMINI-DEBUG [{}]: Conversation enrichment successful", debugId);
            
            return result;
            
        } catch (IllegalArgumentException e) {
            logger.error("GEMINI-DEBUG [{}]: Invalid input for conversation enrichment: {}", debugId, e.getMessage(), e);
            return getDefaultConversationEnrichment();
        } catch (RuntimeException e) {
            logger.error("GEMINI-DEBUG [{}]: API error during conversation enrichment: {}", debugId, e.getMessage(), e);
            return getDefaultConversationEnrichment();
        } catch (Exception e) {
            logger.error("GEMINI-DEBUG [{}]: Unexpected error during conversation enrichment: {}", debugId, e.getMessage(), e);
            return getDefaultConversationEnrichment();
        }
    }
    
    @Override
    public MessageEnrichment enrichMessage(String content, Map<String, Object> context) {
        String debugId = "ENRICH-MSG-" + System.currentTimeMillis();
        try {
            int contextSize = context != null ? context.size() : 0;
            logger.debug("GEMINI-DEBUG [{}]: Starting message enrichment - content length: {}, context size: {}", 
                        debugId, content != null ? content.length() : 0, contextSize);
            
            // Input validation
            if (content == null || content.trim().isEmpty()) {
                logger.warn("GEMINI-DEBUG [{}]: Empty or null content provided for message enrichment", debugId);
                return new MessageEnrichment("General", 0.0, "Unknown", List.of(), 0.0);
            }
            
            if (!isClientAvailable) {
                logger.warn("GEMINI-DEBUG [{}]: Client not available for message enrichment", debugId);
                return new MessageEnrichment("General", 0.0, "Unknown", List.of(), 0.0);
            }
            
            String prompt = buildMessageEnrichmentPrompt(content, context);
            logger.debug("GEMINI-DEBUG [{}]: Built message enrichment prompt, length: {}", debugId, prompt.length());
            
            String response = callGeminiAPI(prompt);
            logger.debug("GEMINI-DEBUG [{}]: Received API response for message enrichment", debugId);
            
            MessageEnrichment result = parseMessageEnrichmentResponse(response);
            logger.debug("GEMINI-DEBUG [{}]: Message enrichment successful", debugId);
            
            return result;
            
        } catch (IllegalArgumentException e) {
            logger.error("GEMINI-DEBUG [{}]: Invalid input for message enrichment: {}", debugId, e.getMessage(), e);
            return new MessageEnrichment("General", 0.0, "Unknown", List.of(), 0.0);
        } catch (RuntimeException e) {
            logger.error("GEMINI-DEBUG [{}]: API error during message enrichment: {}", debugId, e.getMessage(), e);
            return new MessageEnrichment("General", 0.0, "Unknown", List.of(), 0.0);
        } catch (Exception e) {
            logger.error("GEMINI-DEBUG [{}]: Unexpected error during message enrichment: {}", debugId, e.getMessage(), e);
            return new MessageEnrichment("General", 0.0, "Unknown", List.of(), 0.0);
        }
    }
    
    @Override
    public ParticipantInsight analyzeParticipant(SlackParticipant participant, List<SlackMessage> messages) {
        String debugId = "ANALYZE-PARTICIPANT-" + System.currentTimeMillis();
        try {
            String participantId = participant != null ? participant.getId() : "null";
            int messagesCount = messages != null ? messages.size() : 0;
            logger.debug("GEMINI-DEBUG [{}]: Starting participant analysis - participant: {}, total messages: {}", 
                        debugId, participantId, messagesCount);
            
            // Input validation
            if (participant == null) {
                logger.warn("GEMINI-DEBUG [{}]: Null participant provided for analysis", debugId);
                return new ParticipantInsight("Unknown Participant", 0.0, "Neutral", 0);
            }
            
            if (messages == null || messages.isEmpty()) {
                logger.warn("GEMINI-DEBUG [{}]: No messages provided for participant analysis", debugId);
                return new ParticipantInsight("Participant", 0.0, "Neutral", 0);
            }
            
            if (!isClientAvailable) {
                logger.warn("GEMINI-DEBUG [{}]: Client not available for participant analysis", debugId);
                return new ParticipantInsight("Participant", 0.0, "Neutral", 0);
            }
            
            List<SlackMessage> userMessages = messages.stream()
                .filter(msg -> participant.getId().equals(msg.getUserId()))
                .collect(Collectors.toList());
                
            logger.debug("GEMINI-DEBUG [{}]: Filtered to {} messages from participant {}", 
                        debugId, userMessages.size(), participantId);
            
            String prompt = buildParticipantAnalysisPrompt(participant, userMessages);
            logger.debug("GEMINI-DEBUG [{}]: Built participant analysis prompt, length: {}", debugId, prompt.length());
            
            String response = callGeminiAPI(prompt);
            logger.debug("GEMINI-DEBUG [{}]: Received API response for participant analysis", debugId);
            
            ParticipantInsight result = parseParticipantAnalysisResponse(response);
            logger.debug("GEMINI-DEBUG [{}]: Participant analysis successful", debugId);
            
            return result;
            
        } catch (IllegalArgumentException e) {
            logger.error("GEMINI-DEBUG [{}]: Invalid input for participant analysis: {}", debugId, e.getMessage(), e);
            return new ParticipantInsight("Participant", 0.0, "Neutral", 0);
        } catch (RuntimeException e) {
            logger.error("GEMINI-DEBUG [{}]: API error during participant analysis: {}", debugId, e.getMessage(), e);
            return new ParticipantInsight("Participant", 0.0, "Neutral", 0);
        } catch (Exception e) {
            logger.error("GEMINI-DEBUG [{}]: Unexpected error during participant analysis: {}", debugId, e.getMessage(), e);
            return new ParticipantInsight("Participant", 0.0, "Neutral", 0);
        }
    }
    
    @Override
    public UrgencyLevel assessUrgency(List<SlackMessage> messages) {
        String debugId = "ASSESS-URGENCY-" + System.currentTimeMillis();
        try {
            int messagesCount = messages != null ? messages.size() : 0;
            logger.debug("GEMINI-DEBUG [{}]: Starting urgency assessment - {} messages", debugId, messagesCount);
            
            // Input validation
            if (messages == null || messages.isEmpty()) {
                logger.warn("GEMINI-DEBUG [{}]: No messages provided for urgency assessment", debugId);
                return UrgencyLevel.LOW;
            }
            
            if (!isClientAvailable) {
                logger.warn("GEMINI-DEBUG [{}]: Client not available for urgency assessment", debugId);
                return UrgencyLevel.LOW;
            }
            
            String conversationText = formatConversationForAnalysis(messages);
            logger.debug("GEMINI-DEBUG [{}]: Formatted conversation for urgency analysis, length: {}", debugId, conversationText.length());
            
            String prompt = buildUrgencyAssessmentPrompt(conversationText);
            logger.debug("GEMINI-DEBUG [{}]: Built urgency assessment prompt, length: {}", debugId, prompt.length());
            
            String response = callGeminiAPI(prompt);
            logger.debug("GEMINI-DEBUG [{}]: Received API response for urgency assessment", debugId);
            
            UrgencyLevel result = parseUrgencyResponse(response);
            logger.debug("GEMINI-DEBUG [{}]: Urgency assessment successful - level: {}", debugId, result);
            
            return result;
            
        } catch (IllegalArgumentException e) {
            logger.error("GEMINI-DEBUG [{}]: Invalid input for urgency assessment: {}", debugId, e.getMessage(), e);
            return UrgencyLevel.LOW;
        } catch (RuntimeException e) {
            logger.error("GEMINI-DEBUG [{}]: API error during urgency assessment: {}", debugId, e.getMessage(), e);
            return UrgencyLevel.LOW;
        } catch (Exception e) {
            logger.error("GEMINI-DEBUG [{}]: Unexpected error during urgency assessment: {}", debugId, e.getMessage(), e);
            return UrgencyLevel.LOW;
        }
    }
    
    @Override
    public String generateTopic(List<SlackMessage> messages) {
        String debugId = "GENERATE-TOPIC-" + System.currentTimeMillis();
        try {
            int messagesCount = messages != null ? messages.size() : 0;
            logger.debug("GEMINI-DEBUG [{}]: Starting topic generation - {} messages", debugId, messagesCount);
            
            // Input validation
            if (messages == null || messages.isEmpty()) {
                logger.warn("GEMINI-DEBUG [{}]: No messages provided for topic generation", debugId);
                return "General Discussion";
            }
            
            if (!isClientAvailable) {
                logger.warn("GEMINI-DEBUG [{}]: Client not available for topic generation", debugId);
                return "General Discussion";
            }
            
            String conversationText = formatConversationForAnalysis(messages.stream().limit(10).collect(Collectors.toList()));
            logger.debug("GEMINI-DEBUG [{}]: Formatted conversation for topic generation, length: {}", debugId, conversationText.length());
            
            String prompt = buildTopicGenerationPrompt(conversationText);
            logger.debug("GEMINI-DEBUG [{}]: Built topic generation prompt, length: {}", debugId, prompt.length());
            
            String response = callGeminiAPI(prompt);
            logger.debug("GEMINI-DEBUG [{}]: Received API response for topic generation", debugId);
            
            String result = parseTopicResponse(response);
            logger.debug("GEMINI-DEBUG [{}]: Topic generation successful - topic: {}", debugId, result);
            
            return result;
            
        } catch (IllegalArgumentException e) {
            logger.error("GEMINI-DEBUG [{}]: Invalid input for topic generation: {}", debugId, e.getMessage(), e);
            return "General Discussion";
        } catch (RuntimeException e) {
            logger.error("GEMINI-DEBUG [{}]: API error during topic generation: {}", debugId, e.getMessage(), e);
            return "General Discussion";
        } catch (Exception e) {
            logger.error("GEMINI-DEBUG [{}]: Unexpected error during topic generation: {}", debugId, e.getMessage(), e);
            return "General Discussion";
        }
    }
    
    @Override
    public List<String> extractEntities(String content) {
        String debugId = "EXTRACT-ENTITIES-" + System.currentTimeMillis();
        try {
            logger.debug("GEMINI-DEBUG [{}]: Starting entity extraction - content length: {}", debugId, content != null ? content.length() : 0);
            
            // Input validation
            if (content == null || content.trim().isEmpty()) {
                logger.warn("GEMINI-DEBUG [{}]: Empty or null content provided for entity extraction", debugId);
                return List.of();
            }
            
            if (!isClientAvailable) {
                logger.warn("GEMINI-DEBUG [{}]: Client not available for entity extraction", debugId);
                return List.of();
            }
            
            String prompt = buildEntityExtractionPrompt(content);
            logger.debug("GEMINI-DEBUG [{}]: Built entity extraction prompt, length: {}", debugId, prompt.length());
            
            String response = callGeminiAPI(prompt);
            logger.debug("GEMINI-DEBUG [{}]: Received API response for entity extraction", debugId);
            
            List<String> result = parseEntitiesResponse(response);
            logger.debug("GEMINI-DEBUG [{}]: Entity extraction successful - found {} entities", debugId, result.size());
            
            return result;
            
        } catch (IllegalArgumentException e) {
            logger.error("GEMINI-DEBUG [{}]: Invalid input for entity extraction: {}", debugId, e.getMessage(), e);
            return List.of();
        } catch (RuntimeException e) {
            logger.error("GEMINI-DEBUG [{}]: API error during entity extraction: {}", debugId, e.getMessage(), e);
            return List.of();
        } catch (Exception e) {
            logger.error("GEMINI-DEBUG [{}]: Unexpected error during entity extraction: {}", debugId, e.getMessage(), e);
            return List.of();
        }
    }
    
    @Override
    public SentimentResult analyzeSentiment(String content) {
        String debugId = "ANALYZE-SENTIMENT-" + System.currentTimeMillis();
        try {
            logger.debug("GEMINI-DEBUG [{}]: Starting sentiment analysis - content length: {}", debugId, content != null ? content.length() : 0);
            
            // Input validation
            if (content == null || content.trim().isEmpty()) {
                logger.warn("GEMINI-DEBUG [{}]: Empty or null content provided for sentiment analysis", debugId);
                return new SentimentResult("Neutral", 0.0, 0.0);
            }
            
            if (!isClientAvailable) {
                logger.warn("GEMINI-DEBUG [{}]: Client not available for sentiment analysis", debugId);
                return new SentimentResult("Neutral", 0.0, 0.0);
            }
            
            String prompt = buildSentimentAnalysisPrompt(content);
            logger.debug("GEMINI-DEBUG [{}]: Built sentiment analysis prompt, length: {}", debugId, prompt.length());
            
            String response = callGeminiAPI(prompt);
            logger.debug("GEMINI-DEBUG [{}]: Received API response for sentiment analysis", debugId);
            
            SentimentResult result = parseSentimentResponse(response);
            logger.debug("GEMINI-DEBUG [{}]: Sentiment analysis successful - sentiment: {}, confidence: {}", 
                        debugId, result.sentiment(), result.confidence());
            
            return result;
            
        } catch (IllegalArgumentException e) {
            logger.error("GEMINI-DEBUG [{}]: Invalid input for sentiment analysis: {}", debugId, e.getMessage(), e);
            return new SentimentResult("Neutral", 0.0, 0.0);
        } catch (RuntimeException e) {
            logger.error("GEMINI-DEBUG [{}]: API error during sentiment analysis: {}", debugId, e.getMessage(), e);
            return new SentimentResult("Neutral", 0.0, 0.0);
        } catch (Exception e) {
            logger.error("GEMINI-DEBUG [{}]: Unexpected error during sentiment analysis: {}", debugId, e.getMessage(), e);
            return new SentimentResult("Neutral", 0.0, 0.0);
        }
    }
    
    @Override
    public String getProviderId() {
        return "geminiProvider";
    }
    
    @Override
    public boolean isAvailable() {
        return isClientAvailable;
    }
    
    @Override
    public double getLastConfidence() {
        return lastConfidence;
    }
    
    /**
     * Fetches available categories from the data storage service with error handling
     */
    private List<String> fetchAvailableCategories(String debugId) {
        try {
            logger.debug("GEMINI-DEBUG [{}]: Fetching categories from data storage service", debugId);
            APIResponse<List<CategoryDTO>> response = dataStorageServiceClient.getAllCategories(
                defaultTenantId, defaultTenantSchema);
            
            if (response != null && response.getData() != null && !response.getData().isEmpty()) {
                List<String> categoryNames = response.getData().stream()
                    .map(CategoryDTO::getName)
                    .filter(name -> name != null && !name.trim().isEmpty())
                    .toList();
                
                logger.debug("GEMINI-DEBUG [{}]: Successfully fetched {} categories", debugId, categoryNames.size());
                return categoryNames;
            } else {
                logger.warn("GEMINI-DEBUG [{}]: No categories returned from data storage service", debugId);
                return List.of();
            }
            
        } catch (Exception e) {
            logger.warn("GEMINI-DEBUG [{}]: Failed to fetch categories from data storage service: {}", 
                       debugId, e.getMessage(), e);
            return List.of(); // Return empty list to trigger fallback categories
        }
    }
    
    // Private helper methods
    private String callGeminiAPI(String prompt) {
        try {
            if (geminiClient == null) {
                throw new RuntimeException("Gemini client is not available - API key not configured");
            }
            
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
    
    private String buildConversationEnrichmentPrompt(String conversationText, 
                                                    List<SlackParticipant> participants, 
                                                    List<String> availableCategories) {
        StringBuilder participantInfo = new StringBuilder();
        List<String> participantNames = new ArrayList<>();
        
        if (participants != null) {
            participantInfo.append("Participants: ");
            participants.forEach(p -> {
                participantInfo.append(p.getUsername()).append(" ");
                participantNames.add(p.getUsername());
            });
        }
        
        // Build available categories string for the prompt
        String categoriesInstruction = buildCategoriesInstruction(availableCategories);
        
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
            %s
            - For topic.sub-category: Use specific sub-categories that are more detailed than the main categories
            - For conversations: Include 2-5 most relevant messages that capture the essence of the discussion
            - For topic.priority: Base on urgency, impact, and time sensitivity
            - For topic.keywords: Extract 3-7 key terms that best represent the conversation content
            """, 
            participantInfo.toString(), 
            conversationText,
            participantNames.isEmpty() ? "[]" : participantNames.toString(),
            categoriesInstruction);
    }
    
    /**
     * Builds the categories instruction for the prompt based on available categories
     */
    private String buildCategoriesInstruction(List<String> availableCategories) {
        if (availableCategories == null || availableCategories.isEmpty()) {
            return "- For topic.category: Use broad categories like \"Technical\", \"Business\", \"Support\", \"Planning\", etc.";
        }
        
        StringBuilder instruction = new StringBuilder();
        instruction.append("- For topic.category: Choose from these available categories: ");
        instruction.append(String.join(", ", availableCategories.stream()
            .map(cat -> "\"" + cat + "\"")
            .toList()));
        instruction.append(". If none fit perfectly, choose the closest match or use a general category.");
        
        return instruction.toString();
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
                double confidence = parseNumericValue(parts[1].trim(), 0.8);
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
                    sentiment = parseNumericValue(line.substring(10).trim(), 0.0);
                } else if (line.startsWith("Intent:")) {
                    intent = line.substring(7).trim();
                } else if (line.startsWith("Entities:")) {
                    String entitiesStr = line.substring(9).trim();
                    entities = Arrays.asList(entitiesStr.split(",\\s*"));
                } else if (line.startsWith("Confidence:")) {
                    confidence = parseNumericValue(line.substring(11).trim(), 0.5);
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
                    String engagementStr = line.substring(11).trim();
                    engagement = parseEngagementValue(engagementStr);
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
    
    /**
     * Extracts numeric engagement value from a string that may contain descriptive text.
     * Handles formats like:
     * - "0.2"
     * - "0.2 (Very low. One message only...)"
     * - "0.8 - High engagement"
     */
    private double parseEngagementValue(String engagementStr) {
        try {
            // First, try to parse directly in case it's just a number
            return Double.parseDouble(engagementStr);
        } catch (NumberFormatException e) {
            // If that fails, extract the first numeric value from the string
            try {
                // Use regex to find first decimal number (including integers)
                java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("([0-9]*\\.?[0-9]+)");
                java.util.regex.Matcher matcher = pattern.matcher(engagementStr);
                
                if (matcher.find()) {
                    double value = Double.parseDouble(matcher.group(1));
                    // Ensure the value is within valid range [0.0, 1.0]
                    return Math.max(0.0, Math.min(1.0, value));
                } else {
                    logger.warn("No numeric value found in engagement string: {}", engagementStr);
                    return 0.5; // Default fallback
                }
            } catch (Exception parseException) {
                logger.warn("Failed to extract numeric value from engagement string: {}", engagementStr, parseException);
                return 0.5; // Default fallback
            }
        }
    }
    
    /**
     * Extracts numeric value from a string that may contain descriptive text.
     * Handles formats like:
     * - "0.8"
     * - "0.8 (High confidence)"
     * - "0.2 - Low value with description"
     */
    private double parseNumericValue(String valueStr, double defaultValue) {
        try {
            // First, try to parse directly in case it's just a number
            return Double.parseDouble(valueStr);
        } catch (NumberFormatException e) {
            // If that fails, extract the first numeric value from the string
            try {
                // Use regex to find first decimal number (including integers)
                java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("([0-9]*\\.?[0-9]+)");
                java.util.regex.Matcher matcher = pattern.matcher(valueStr);
                
                if (matcher.find()) {
                    double value = Double.parseDouble(matcher.group(1));
                    return value;
                } else {
                    logger.warn("No numeric value found in string: {}", valueStr);
                    return defaultValue; // Default fallback
                }
            } catch (Exception parseException) {
                logger.warn("Failed to extract numeric value from string: {}", valueStr, parseException);
                return defaultValue; // Default fallback
            }
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
                    score = parseNumericValue(line.substring(6).trim(), 0.0);
                } else if (line.startsWith("Confidence:")) {
                    confidence = parseNumericValue(line.substring(11).trim(), 0.5);
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
