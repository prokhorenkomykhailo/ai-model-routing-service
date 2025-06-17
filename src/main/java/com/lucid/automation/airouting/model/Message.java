package com.lucid.automation.airouting.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.index.Indexed;

import java.io.Serializable;
import java.util.Map;

/**
 * Model representing a message stored in Redis
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@RedisHash("message")
public class Message implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    /**
     * Unique ID generated as: tenantId:workspaceId:channelId:threadTs:messageTs
     */
    @Id
    private String id;
    
    /**
     * Tenant ID for multi-tenant support
     */
    @Indexed
    private String tenantId;
    
    /**
     * Workspace/Team ID
     */
    @Indexed
    private String workspaceId;
    
    /**
     * Channel ID where the message was posted
     */
    @Indexed
    private String channelId;
    
    /**
     * Thread timestamp (if part of a thread)
     */
    @Indexed
    private String threadTs;
    
    /**
     * Message timestamp (unique within a channel)
     */
    @Indexed
    private String messageTs;
    
    // Composite indexes for common query patterns
    
    /**
     * Composite index for tenant + workspace queries
     * Format: "tenantId:workspaceId"
     */
    @Indexed
    private String tenantWorkspaceIndex;
    
    /**
     * Composite index for tenant + workspace + channel queries
     * Format: "tenantId:workspaceId:channelId"
     */
    @Indexed
    private String tenantWorkspaceChannelIndex;
    
    /**
     * Composite index for tenant + workspace + channel + thread queries
     * Format: "tenantId:workspaceId:channelId:threadTs"
     */
    @Indexed
    private String tenantWorkspaceChannelThreadIndex;
    
    /**
     * Composite index for workspace + channel + thread queries
     * Format: "workspaceId:channelId:threadTs"
     */
    @Indexed
    private String workspaceChannelThreadIndex;
    
    /**
     * User ID who sent the message
     */
    private String userId;
    
    /**
     * User name who sent the message
     */
    private String username;
    
    /**
     * Message text content
     */
    private String text;
    
    /**
     * Message type (message, system, etc.)
     */
    private String messageType;
    
    /**
     * Message subtype (channel_join, channel_leave, etc.)
     */
    private String subtype;
    
    /**
     * Additional metadata for the message
     */
    private Map<String, Object> metadata;
    
    /**
     * Timestamp when message was ingested
     */
    private Long ingestedAt;
    
    /**
     * Update composite indexes when core fields change
     * This method should be called after setting tenantId, workspaceId, channelId, or threadTs
     */
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
    
    /**
     * Override setters to automatically update composite indexes
     */
    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
        updateCompositeIndexes();
    }
    
    public void setWorkspaceId(String workspaceId) {
        this.workspaceId = workspaceId;
        updateCompositeIndexes();
    }
    
    public void setChannelId(String channelId) {
        this.channelId = channelId;
        updateCompositeIndexes();
    }
    
    public void setThreadTs(String threadTs) {
        this.threadTs = threadTs;
        updateCompositeIndexes();
    }
}
