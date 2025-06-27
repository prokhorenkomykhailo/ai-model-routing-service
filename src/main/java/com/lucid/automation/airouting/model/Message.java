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
     * Channel name where the message was posted
     */
    @Indexed
    private String channelName;
    
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
    
    /**
     * Tenant schema for multi-tenancy support
     */
    @Indexed
    private String tenantSchema;
    
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
    private String username;    // slack username
    
    /**
     * Deemerge user ID (for user disambiguation or mapping)
     */
    @Indexed
    private String deemergeUserId;
    
    // User profile fields from UserData
    /**
     * Slack user ID
     */
    private String slackUserId;
    
    /**
     * Team ID the user belongs to
     */
    private String teamId;
    
    /**
     * User's full name
     */
    private String name;
    
    /**
     * Whether user's email is confirmed
     */
    private Boolean emailConfirmed;
    
    /**
     * User's display name
     */
    private String displayName;
    
    /**
     * Normalized display name
     */
    private String displayNameNormalized;
    
    /**
     * Normalized real name
     */
    private String realNameNormalized;
    
    /**
     * User's email address
     */
    private String email;
    
    /**
     * User's job title
     */
    private String title;
    
    /**
     * User's phone number
     */
    private String phone;
    
    /**
     * User's first name
     */
    private String firstName;
    
    /**
     * User's last name
     */
    private String lastName;
    
    /**
     * User's pronouns
     */
    private String pronouns;
    
    /**
     * User's status text
     */
    private String statusText;
    
    /**
     * User's avatar hash
     */
    private String avatarHash;
    
    /**
     * User's original image URL
     */
    private String imageOriginal;
    
    /**
     * User's 24px image URL
     */
    private String image24;
    
    /**
     * User's 32px image URL
     */
    private String image32;
    
    /**
     * User's 48px image URL
     */
    private String image48;
    
    /**
     * User's 72px image URL
     */
    private String image72;
    
    /**
     * User's 192px image URL
     */
    private String image192;
    
    /**
     * User's 512px image URL
     */
    private String image512;
    
    /**
     * User's 1024px image URL
     */
    private String image1024;
    
    /**
     * Team name the user belongs to
     */
    private String teamName;
    
    /**
     * Slack updated timestamp for the user
     */
    private Long slackUpdatedAt;
    
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
