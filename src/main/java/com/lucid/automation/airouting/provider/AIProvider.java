package com.lucid.automation.airouting.provider;

import com.lucid.automation.airouting.model.message.AIMessage;

import java.util.Map;

public interface AIProvider {
    
    /**
     * Enrich an entire conversation with dynamic categories
     * 
     * @param messages The AI message containing the list of Slack messages to analyze and context
     * @return Map containing the analysis results
     */
    Map<String, Object> enrichConversation(AIMessage messages);
    
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
