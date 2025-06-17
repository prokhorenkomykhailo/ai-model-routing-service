package com.lucid.automation.airouting.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * Data Transfer Object for messages received from the ingestion queue
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class IngestionEventDTO {
    private String tenantId;
    private String tenantSchema;
    private MessageData message;
    private Double ingestedAt;
    
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
