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
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

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
    
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class SlackMessageDTO {
        @JsonProperty("type")
        private String type;
        
        @JsonProperty("subtype")
        private String subtype;
        
        @JsonProperty("user")
        private String user;
        
        @JsonProperty("username")
        private String username;
        
        @JsonProperty("text")
        private String text;
        
        @JsonProperty("ts")
        private String ts;
        
        @JsonProperty("channelId")
        private String channelId;
        
        @JsonProperty("teamId")
        private String teamId;
        
        @JsonProperty("threadTs")
        private String threadTs;
        
        @JsonProperty("conversationGroupId")
        private String conversationGroupId;
        
        @JsonProperty("ingestedAt")
        private Double ingestedAt;
        
        @JsonProperty("topic")
        private String topic;
        
        @JsonProperty("purpose")
        private String purpose;
        
        @JsonProperty("clientMsgId")
        private String clientMsgId;
        
        @JsonProperty("replyCount")
        private Integer replyCount;
        
        @JsonProperty("replyUsers")
        private List<String> replyUsers;
        
        @JsonProperty("replyUsersCount")
        private Integer replyUsersCount;
        
        @JsonProperty("latestReply")
        private String latestReply;
    }
    
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class SlackUserDTO {
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
    
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class SlackMessageMetadataDTO {
        @JsonProperty("channelName")
        private String channelName;
        
        @JsonProperty("channelType")
        private String channelType;
        
        @JsonProperty("workspaceName")
        private String workspaceName;
        
        @JsonProperty("isThreadMessage")
        private boolean isThreadMessage;
        
        @JsonProperty("hasAttachments")
        private boolean hasAttachments;
        
        @JsonProperty("mentionedUsers")
        private List<String> mentionedUsers;
        
        @JsonProperty("hasReactions")
        private boolean hasReactions;
        
        @JsonProperty("messageLength")
        private int messageLength;
        
        @JsonProperty("containsUrls")
        private boolean containsUrls;
        
        @JsonProperty("priority")
        private String priority;
        
        @JsonProperty("source")
        private String source;
        
        @JsonProperty("additionalAttributes")
        private Map<String, Object> additionalAttributes;
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
}
