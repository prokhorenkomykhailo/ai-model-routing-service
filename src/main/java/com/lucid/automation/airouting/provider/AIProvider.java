package com.lucid.automation.airouting.provider;

import com.lucid.automation.airouting.dto.*;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.SlackParticipant;

import java.util.List;
import java.util.Map;

public interface AIProvider {
    
    /**
     * Categorize a single message
     */
    CategoryResult categorize(String content);
    
    /**
     * Summarize content
     */
    SummaryResult summarize(String content);
    
    /**
     * Enrich an entire conversation with dynamic categories
     * 
     * @param messages The list of Slack messages to analyze
     * @param participants The list of participants in the conversation
     * @param availableCategories The list of available categories to use for categorization.
     *                           If null or empty, implementations should use their default behavior.
     * @return ConversationEnrichment object containing the analysis results
     */
    ConversationEnrichment enrichConversation(List<SlackMessage> messages, 
                                             List<SlackParticipant> participants, 
                                             List<String> availableCategories);
    
    /**
     * Enrich a single message
     */
    MessageEnrichment enrichMessage(String content, Map<String, Object> context);
    
    /**
     * Analyze a participant's behavior
     */
    ParticipantInsight analyzeParticipant(SlackParticipant participant, List<SlackMessage> messages);
    
    /**
     * Assess urgency of messages
     */
    UrgencyLevel assessUrgency(List<SlackMessage> messages);
    
    /**
     * Generate topic from messages
     */
    String generateTopic(List<SlackMessage> messages);
    
    /**
     * Extract entities from content
     */
    List<String> extractEntities(String content);
    
    /**
     * Analyze sentiment
     */
    SentimentResult analyzeSentiment(String content);
    
    /**
     * Get provider identifier
     */
    String getProviderId();
    
    /**
     * Check if provider is available
     */
    boolean isAvailable();
    
    /**
     * Get last confidence score
     */
    double getLastConfidence();
}
