package com.lucid.automation.airouting.provider;

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
     * Enrich an entire conversation
     */
    ConversationEnrichment enrichConversation(List<SlackMessage> messages, List<SlackParticipant> participants);
    
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
    
    // Result classes
    record CategoryResult(String category, double confidence) {}
    record SummaryResult(String summary, String metadata) {}
    record SentimentResult(String sentiment, double score, double confidence) {}
    
    enum UrgencyLevel {
        LOW(1), MEDIUM(2), HIGH(3), CRITICAL(4);
        
        private final int priority;
        UrgencyLevel(int priority) { this.priority = priority; }
        public int getPriority() { return priority; }
    }
    
    record MessageEnrichment(
        String category,
        double sentiment,
        String intent,
        List<String> entities,
        double confidence
    ) {}
    
    record ParticipantInsight(
        String role,
        double engagementLevel,
        String dominantSentiment,
        int messageCount
    ) {}
    
    record ConversationEnrichment(
        String topic,
        String summary,
        UrgencyLevel urgency,
        List<ParticipantInsight> participants,
        List<MessageEnrichment> messages,
        Map<String, Object> metadata
    ) {}
}
