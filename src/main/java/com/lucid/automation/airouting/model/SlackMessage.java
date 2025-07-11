package com.lucid.automation.airouting.model;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public class SlackMessage {
    // Core message fields
    private String id;
    private String content;
    private String userId;
    private String username;
    private LocalDateTime timestamp;
    private String channelId;
    private String threadId;
    
    // Fields from Message class
    private String tenantId;
    private String workspaceId;
    private String threadTs;
    private String messageTs;
    
    // Composite indexes for common query patterns
    private String tenantWorkspaceIndex;
    private String tenantWorkspaceChannelIndex;
    private String tenantWorkspaceChannelThreadIndex;
    private String workspaceChannelThreadIndex;
    
    // User profile fields from Message
    private String slackUserId;
    private String teamId;
    private String name;
    private Boolean emailConfirmed;
    private String displayName;
    private String displayNameNormalized;
    private String realNameNormalized;
    private String email;
    private String title;
    private String phone;
    private String firstName;
    private String lastName;
    private String pronouns;
    private String statusText;
    private String avatarHash;
    private String imageOriginal;
    private String image24;
    private String image32;
    private String image48;
    private String image72;
    private String image192;
    private String image512;
    private String image1024;
    private String teamName;
    private Long slackUpdatedAt;
    
    // Message content and metadata
    private String text;
    private String messageType;
    private Map<String, Object> metadata;
    private Long ingestedAt;
    
    // Additional Slack-specific fields
    private String type;
    private String subtype;
    private String user;
    private String ts;
    private String channel;
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
    
    // Update composite indexes when core fields change
    public void updateCompositeIndexes() {
        if (tenantId != null && workspaceId != null) {
            this.tenantWorkspaceIndex = tenantId + ":" + workspaceId;
            
            if (channelId != null) {
                this.tenantWorkspaceChannelIndex = tenantId + ":" + workspaceId + ":" + channelId;
                
                if (threadTs != null) {
                    this.tenantWorkspaceChannelThreadIndex = tenantId + ":" + workspaceId + ":" + channelId + ":" + threadTs;
                }
            }
        }
        
        if (workspaceId != null && channelId != null && threadTs != null) {
            this.workspaceChannelThreadIndex = workspaceId + ":" + channelId + ":" + threadTs;
        }
    }
    
    // Core message field getters and setters
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
    public void setChannelId(String channelId) { 
        this.channelId = channelId; 
        updateCompositeIndexes();
    }
    
    public String getThreadId() { return threadId; }
    public void setThreadId(String threadId) { this.threadId = threadId; }
    
    // Fields from Message class - getters and setters
    public String getTenantId() { return tenantId; }
    public void setTenantId(String tenantId) { 
        this.tenantId = tenantId; 
        updateCompositeIndexes();
    }
    
    public String getWorkspaceId() { return workspaceId; }
    public void setWorkspaceId(String workspaceId) { 
        this.workspaceId = workspaceId; 
        updateCompositeIndexes();
    }
    
    public String getThreadTs() { return threadTs; }
    public void setThreadTs(String threadTs) { 
        this.threadTs = threadTs; 
        updateCompositeIndexes();
    }
    
    public String getMessageTs() { return messageTs; }
    public void setMessageTs(String messageTs) { this.messageTs = messageTs; }
    
    // Composite indexes getters and setters
    public String getTenantWorkspaceIndex() { return tenantWorkspaceIndex; }
    public void setTenantWorkspaceIndex(String tenantWorkspaceIndex) { this.tenantWorkspaceIndex = tenantWorkspaceIndex; }
    
    public String getTenantWorkspaceChannelIndex() { return tenantWorkspaceChannelIndex; }
    public void setTenantWorkspaceChannelIndex(String tenantWorkspaceChannelIndex) { this.tenantWorkspaceChannelIndex = tenantWorkspaceChannelIndex; }
    
    public String getTenantWorkspaceChannelThreadIndex() { return tenantWorkspaceChannelThreadIndex; }
    public void setTenantWorkspaceChannelThreadIndex(String tenantWorkspaceChannelThreadIndex) { this.tenantWorkspaceChannelThreadIndex = tenantWorkspaceChannelThreadIndex; }
    
    public String getWorkspaceChannelThreadIndex() { return workspaceChannelThreadIndex; }
    public void setWorkspaceChannelThreadIndex(String workspaceChannelThreadIndex) { this.workspaceChannelThreadIndex = workspaceChannelThreadIndex; }
    
    // User profile fields getters and setters
    public String getSlackUserId() { return slackUserId; }
    public void setSlackUserId(String slackUserId) { this.slackUserId = slackUserId; }
    
    public String getTeamId() { return teamId; }
    public void setTeamId(String teamId) { this.teamId = teamId; }
    
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    
    public Boolean getEmailConfirmed() { return emailConfirmed; }
    public void setEmailConfirmed(Boolean emailConfirmed) { this.emailConfirmed = emailConfirmed; }
    
    public String getDisplayName() { return displayName; }
    public void setDisplayName(String displayName) { this.displayName = displayName; }
    
    public String getDisplayNameNormalized() { return displayNameNormalized; }
    public void setDisplayNameNormalized(String displayNameNormalized) { this.displayNameNormalized = displayNameNormalized; }
    
    public String getRealNameNormalized() { return realNameNormalized; }
    public void setRealNameNormalized(String realNameNormalized) { this.realNameNormalized = realNameNormalized; }
    
    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
    
    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }
    
    public String getPhone() { return phone; }
    public void setPhone(String phone) { this.phone = phone; }
    
    public String getFirstName() { return firstName; }
    public void setFirstName(String firstName) { this.firstName = firstName; }
    
    public String getLastName() { return lastName; }
    public void setLastName(String lastName) { this.lastName = lastName; }
    
    public String getPronouns() { return pronouns; }
    public void setPronouns(String pronouns) { this.pronouns = pronouns; }
    
    public String getStatusText() { return statusText; }
    public void setStatusText(String statusText) { this.statusText = statusText; }
    
    public String getAvatarHash() { return avatarHash; }
    public void setAvatarHash(String avatarHash) { this.avatarHash = avatarHash; }
    
    public String getImageOriginal() { return imageOriginal; }
    public void setImageOriginal(String imageOriginal) { this.imageOriginal = imageOriginal; }
    
    public String getImage24() { return image24; }
    public void setImage24(String image24) { this.image24 = image24; }
    
    public String getImage32() { return image32; }
    public void setImage32(String image32) { this.image32 = image32; }
    
    public String getImage48() { return image48; }
    public void setImage48(String image48) { this.image48 = image48; }
    
    public String getImage72() { return image72; }
    public void setImage72(String image72) { this.image72 = image72; }
    
    public String getImage192() { return image192; }
    public void setImage192(String image192) { this.image192 = image192; }
    
    public String getImage512() { return image512; }
    public void setImage512(String image512) { this.image512 = image512; }
    
    public String getImage1024() { return image1024; }
    public void setImage1024(String image1024) { this.image1024 = image1024; }
    
    public String getTeamName() { return teamName; }
    public void setTeamName(String teamName) { this.teamName = teamName; }
    
    public Long getSlackUpdatedAt() { return slackUpdatedAt; }
    public void setSlackUpdatedAt(Long slackUpdatedAt) { this.slackUpdatedAt = slackUpdatedAt; }
    
    // Message content and metadata getters and setters
    public String getText() { return text; }
    public void setText(String text) { this.text = text; }
    
    public String getMessageType() { return messageType; }
    public void setMessageType(String messageType) { this.messageType = messageType; }
    
    public Map<String, Object> getMetadata() { return metadata; }
    public void setMetadata(Map<String, Object> metadata) { this.metadata = metadata; }
    
    public Long getIngestedAt() { return ingestedAt; }
    public void setIngestedAt(Long ingestedAt) { this.ingestedAt = ingestedAt; }
    
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
