package com.lucid.automation.airouting.model;

import java.time.LocalDateTime;
import java.util.List;

public class SlackMessage {
    private String id;
    private String content;
    private String userId;
    private String username;
    private LocalDateTime timestamp;
    private String channelId;
    private String threadId;
    
    // Additional Slack-specific fields
    private String type;
    private String subtype;
    private String user;
    private String ts;
    private String channel;
    private String text;
    private String threadTs;
    private int replyCount;
    private List<Object> replies;
    private List<Object> reactions;
    private List<Object> files;
    private List<Object> attachments;
    
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
    
    // Additional Slack-specific getters and setters
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    
    public String getSubtype() { return subtype; }
    public void setSubtype(String subtype) { this.subtype = subtype; }
    
    public String getUser() { return user; }
    public void setUser(String user) { this.user = user; }
    
    public String getTs() { return ts; }
    public void setTs(String ts) { this.ts = ts; }
    
    public String getChannel() { return channel; }
    public void setChannel(String channel) { this.channel = channel; }
    
    public String getText() { return text; }
    public void setText(String text) { this.text = text; }
    
    public String getThreadTs() { return threadTs; }
    public void setThreadTs(String threadTs) { this.threadTs = threadTs; }
    
    public int getReplyCount() { return replyCount; }
    public void setReplyCount(int replyCount) { this.replyCount = replyCount; }
    
    public List<Object> getReplies() { return replies; }
    public void setReplies(List<Object> replies) { this.replies = replies; }
    
    public List<Object> getReactions() { return reactions; }
    public void setReactions(List<Object> reactions) { this.reactions = reactions; }
    
    public List<Object> getFiles() { return files; }
    public void setFiles(List<Object> files) { this.files = files; }
    
    public List<Object> getAttachments() { return attachments; }
    public void setAttachments(List<Object> attachments) { this.attachments = attachments; }
}
