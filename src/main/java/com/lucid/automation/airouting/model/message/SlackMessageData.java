package com.lucid.automation.airouting.model.message;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;

/**
 * Simplified message data for RabbitMQ processing
 */
public class SlackMessageData {
    
    @JsonProperty("id")
    private String id;
    
    @JsonProperty("userId")
    private String userId;
    
    @JsonProperty("content")
    private String content;
    
    @JsonProperty("timestamp")
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime timestamp;
    
    @JsonProperty("channelId")
    private String channelId;
    
    @JsonProperty("threadTs")
    private String threadTs;
    
    // Constructors
    public SlackMessageData() {}
    
    public SlackMessageData(String id, String userId, String content) {
        this.id = id;
        this.userId = userId;
        this.content = content;
        this.timestamp = LocalDateTime.now();
    }
    
    // Getters and Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    
    public String getContent() { return content; }
    public void setContent(String content) { this.content = content; }
    
    public LocalDateTime getTimestamp() { return timestamp; }
    public void setTimestamp(LocalDateTime timestamp) { this.timestamp = timestamp; }
    
    public String getChannelId() { return channelId; }
    public void setChannelId(String channelId) { this.channelId = channelId; }
    
    public String getThreadTs() { return threadTs; }
    public void setThreadTs(String threadTs) { this.threadTs = threadTs; }
    
    @Override
    public String toString() {
        return "SlackMessageData{" +
                "id='" + id + '\'' +
                ", userId='" + userId + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
