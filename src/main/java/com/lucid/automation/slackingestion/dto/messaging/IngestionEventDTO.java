package com.lucid.automation.slackingestion.dto.messaging;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * DTO for consuming messages from Kafka for AI processing
 * Matches the structure from lucid-slack-ingestion-service
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonTypeInfo(use = JsonTypeInfo.Id.NONE) // Ignore type information
public class IngestionEventDTO {
    
    @JsonProperty("tenantId")
    private String tenantId;
    
    @JsonProperty("tenantSchema")
    private String tenantSchema;
    
    @JsonProperty("message")
    private SlackMessageDTO message;
    
    @JsonProperty("user")
    private SlackUserDTO user;
    
    @JsonProperty("metadata")
    private SlackMessageMetadataDTO metadata;
    
    @JsonProperty("ingestedAt")
    private Instant ingestedAt;
    
    // Helper methods to maintain backward compatibility with existing code
    public String getMessageId() {
        return message != null ? message.getTeamId() + "_" + message.getChannelId() + "_" + message.getTs() : null;
    }
    
    public String getTeamId() {
        return message != null ? message.getTeamId() : null;
    }
    
    public String getChannelId() {
        return message != null ? message.getChannelId() : null;
    }
    
    public String getUserId() {
        return message != null ? message.getUser() : null;
    }
    
    public String getUsername() {
        return message != null ? message.getUsername() : null;
    }
    
    public String getText() {
        return message != null ? message.getText() : null;
    }
    
    public String getTimestamp() {
        return message != null ? message.getTs() : null;
    }
    
    public String getThreadTs() {
        return message != null ? message.getThreadTs() : null;
    }
    
    public String getMessageType() {
        return message != null ? message.getType() : null;
    }
    
    public String getSubtype() {
        return message != null ? message.getSubtype() : null;
    }
    
    public String getConversationGroupId() {
        return message != null ? message.getConversationGroupId() : null;
    }
}
