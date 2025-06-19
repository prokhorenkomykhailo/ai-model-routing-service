package com.lucid.automation.airouting.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * DTO representing a conversation message with full user context
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public record ConversationMessage(
    @JsonProperty("id")
    String id,
    
    @JsonProperty("username")
    String username,
    
    @JsonProperty("displayName")
    String displayName,
    
    @JsonProperty("sender")
    String sender,
    
    @JsonProperty("imageUrl")
    String imageUrl,
    
    @JsonProperty("text")
    String text,
    
    @JsonProperty("timestamp")
    String timestamp,
    
    @JsonProperty("source")
    String source,
    
    @JsonProperty("messageType")
    String messageType,
    
    @JsonProperty("isRelevantToTopic")
    Boolean isRelevantToTopic,
    
    @JsonProperty("relevance")
    String relevance
) {
    
    /**
     * Create ConversationMessage from old format (for backward compatibility)
     */
    public static ConversationMessage fromLegacy(String text, String relevance) {
        return new ConversationMessage(
            null, // id
            null, // username
            null, // displayName
            null, // sender
            null, // imageUrl
            text,
            null, // timestamp
            "legacy", // source
            null, // messageType
            null, // isRelevantToTopic
            relevance
        );
    }
}