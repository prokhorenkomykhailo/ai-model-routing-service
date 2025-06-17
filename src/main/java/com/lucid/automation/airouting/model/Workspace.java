package com.lucid.automation.airouting.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.index.Indexed;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

/**
 * Workspace entity representing a Slack workspace/team
 */
@RedisHash("workspace")
public class Workspace {
    
    @Id
    private String id; // This will be the workspaceId (teamId from Slack)
    
    @Indexed
    private String tenantId;
    
    @Indexed
    private String tenantSchema;
    
    private String name;
    private String domain;
    private String teamId; // Slack team ID (same as id, kept for clarity)
    
    @Indexed
    private Instant firstMessageAt;
    
    @Indexed
    private Instant lastMessageAt;
    
    private Long totalMessages;
    private Long totalChannels;
    private Long totalThreads;
    
    private Set<String> channelIds;
    
    @Indexed
    private Instant createdAt;
    
    @Indexed
    private Instant updatedAt;
    
    // Constructors
    public Workspace() {
        this.channelIds = new HashSet<>();
        this.totalMessages = 0L;
        this.totalChannels = 0L;
        this.totalThreads = 0L;
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
    }
    
    public Workspace(String workspaceId, String tenantId) {
        this();
        this.id = workspaceId;
        this.tenantId = tenantId;
        this.teamId = workspaceId;
    }
    
    public Workspace(String workspaceId, String tenantId, String tenantSchema) {
        this();
        this.id = workspaceId;
        this.tenantId = tenantId;
        this.tenantSchema = tenantSchema;
        this.teamId = workspaceId;
    }
    
    // Getters and Setters
    public String getId() { return id; }
    public void setId(String id) { 
        this.id = id; 
        this.teamId = id; // Keep teamId in sync
    }
    
    public String getTenantId() { return tenantId; }
    public void setTenantId(String tenantId) { this.tenantId = tenantId; }
    
    public String getTenantSchema() { return tenantSchema; }
    public void setTenantSchema(String tenantSchema) { this.tenantSchema = tenantSchema; }
    
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    
    public String getDomain() { return domain; }
    public void setDomain(String domain) { this.domain = domain; }
    
    public String getTeamId() { return teamId; }
    public void setTeamId(String teamId) { 
        this.teamId = teamId;
        if (this.id == null) {
            this.id = teamId;
        }
    }
    
    public Instant getFirstMessageAt() { return firstMessageAt; }
    public void setFirstMessageAt(Instant firstMessageAt) { this.firstMessageAt = firstMessageAt; }
    
    public Instant getLastMessageAt() { return lastMessageAt; }
    public void setLastMessageAt(Instant lastMessageAt) { this.lastMessageAt = lastMessageAt; }
    
    public Long getTotalMessages() { return totalMessages; }
    public void setTotalMessages(Long totalMessages) { this.totalMessages = totalMessages; }
    
    public Long getTotalChannels() { return totalChannels; }
    public void setTotalChannels(Long totalChannels) { this.totalChannels = totalChannels; }
    
    public Long getTotalThreads() { return totalThreads; }
    public void setTotalThreads(Long totalThreads) { this.totalThreads = totalThreads; }
    
    public Set<String> getChannelIds() { return channelIds; }
    public void setChannelIds(Set<String> channelIds) { 
        this.channelIds = channelIds != null ? channelIds : new HashSet<>(); 
        this.totalChannels = (long) this.channelIds.size();
    }
    
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    
    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
    
    // Helper methods
    public void addChannel(String channelId) {
        if (channelId != null && !channelId.trim().isEmpty()) {
            this.channelIds.add(channelId);
            this.totalChannels = (long) this.channelIds.size();
            this.updatedAt = Instant.now();
        }
    }
    
    public void incrementMessageCount() {
        this.totalMessages++;
        this.updatedAt = Instant.now();
    }
    
    public void incrementThreadCount() {
        this.totalThreads++;
        this.updatedAt = Instant.now();
    }
    
    public void updateLastMessageTime(Instant messageTime) {
        if (messageTime != null) {
            if (this.firstMessageAt == null || messageTime.isBefore(this.firstMessageAt)) {
                this.firstMessageAt = messageTime;
            }
            if (this.lastMessageAt == null || messageTime.isAfter(this.lastMessageAt)) {
                this.lastMessageAt = messageTime;
            }
        }
        this.updatedAt = Instant.now();
    }
    
    @Override
    public String toString() {
        return String.format("Workspace{id='%s', tenantId='%s', name='%s', totalMessages=%d, totalChannels=%d}", 
                           id, tenantId, name, totalMessages, totalChannels);
    }
}
