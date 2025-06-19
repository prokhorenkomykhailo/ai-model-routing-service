package com.lucid.automation.airouting.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * DTO for publishing messages to RabbitMQ for AI processing
 * Matches the structure from lucid-slack-ingestion-service
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class IngestionEventDTO {
    
    @JsonProperty("tenantId")
    private String tenantId;
    
    @JsonProperty("tenantSchema")
    private String tenantSchema;
    
    @JsonProperty("message")
    private MessageData message;
    
    @JsonProperty("user")
    private UserData user;
    
    @JsonProperty("ingestedAt")
    private Instant ingestedAt;
    
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class MessageData {
        private String type;
        private String subtype;
        private String user;
        private String username;
        private String text;
        private String ts;
        private String channelId;
        private String teamId;
        private String threadTs;
        private String conversationGroupId;
        private Double ingestedAt;
        private String topic;
        private String purpose;
        private String clientMsgId;
        private Integer replyCount;
        private List<String> replyUsers;
        private Integer replyUsersCount;
        private String latestReply;
        
        // Helper methods to maintain backward compatibility
        public String getUserId() {
            return user;
        }
        
        public String getTimestamp() {
            return ts;
        }
        
        public String getMessageType() {
            return type;
        }
    }
    
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class UserData {
        @JsonProperty("id")
        private Long id;
        
        @JsonProperty("slackUserId")
        private String slackUserId;
        
        @JsonProperty("teamId")
        private String teamId;
        
        @JsonProperty("name")
        private String name;
        
        @JsonProperty("emailConfirmed")
        private Boolean emailConfirmed;
        
        @JsonProperty("displayName")
        private String displayName;
        
        @JsonProperty("displayNameNormalized")
        private String displayNameNormalized;
        
        @JsonProperty("realNameNormalized")
        private String realNameNormalized;
        
        @JsonProperty("email")
        private String email;
        
        @JsonProperty("title")
        private String title;
        
        @JsonProperty("phone")
        private String phone;
        
        @JsonProperty("firstName")
        private String firstName;
        
        @JsonProperty("lastName")
        private String lastName;
        
        @JsonProperty("pronouns")
        private String pronouns;
        
        @JsonProperty("statusText")
        private String statusText;
        
        @JsonProperty("avatarHash")
        private String avatarHash;
        
        @JsonProperty("imageOriginal")
        private String imageOriginal;
        
        @JsonProperty("image24")
        private String image24;
        
        @JsonProperty("image32")
        private String image32;
        
        @JsonProperty("image48")
        private String image48;
        
        @JsonProperty("image72")
        private String image72;
        
        @JsonProperty("image192")
        private String image192;
        
        @JsonProperty("image512")
        private String image512;
        
        @JsonProperty("image1024")
        private String image1024;
        
        @JsonProperty("teamName")
        private String teamName;
        
        @JsonProperty("slackUpdatedAt")
        private Long slackUpdatedAt;
        
        @JsonProperty("createdAt")
        private LocalDateTime createdAt;
        
        @JsonProperty("updatedAt")
        private LocalDateTime updatedAt;
    }
    
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
    
    public MessageMetadata getMetadata() {
        if (message == null) return null;
        
        MessageMetadata metadata = new MessageMetadata();
        metadata.setThreadMessage(message.getThreadTs() != null);
        metadata.setMessageLength(message.getText() != null ? message.getText().length() : 0);
        return metadata;
    }
    
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class MessageMetadata {
        private String channelName;
        private String channelType;
        private String workspaceName;
        private boolean isThreadMessage;
        private boolean hasAttachments;
        private List<String> mentionedUsers;
        private boolean hasReactions;
        private int messageLength;
        private boolean containsUrls;
        private String priority;
        private String source;
        private Map<String, Object> additionalAttributes;
    }
}
