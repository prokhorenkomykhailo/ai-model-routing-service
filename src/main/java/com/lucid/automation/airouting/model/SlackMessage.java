package com.lucid.automation.airouting.model;

import java.time.LocalDateTime;

public class SlackMessage {
    private String id;
    private String content;
    private String userId;
    private String username;
    private LocalDateTime timestamp;
    private String channelId;
    private String threadId;
    
    // Constructors
    public SlackMessage() {}
    
    public SlackMessage(String id, String content, String userId, String username, LocalDateTime timestamp) {
        this.id = id;
        this.content = content;
        this.userId = userId;
        this.username = username;
        this.timestamp = timestamp;
    }
    
    // Getters and Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    
    public String getContent() { return content; }
    public void setContent(String content) { this.content = content; }
    
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    
    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }
    
    public LocalDateTime getTimestamp() { return timestamp; }
    public void setTimestamp(LocalDateTime timestamp) { this.timestamp = timestamp; }
    
    public String getChannelId() { return channelId; }
    public void setChannelId(String channelId) { this.channelId = channelId; }
    
    public String getThreadId() { return threadId; }
    public void setThreadId(String threadId) { this.threadId = threadId; }
}
