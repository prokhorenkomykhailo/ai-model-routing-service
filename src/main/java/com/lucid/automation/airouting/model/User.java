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
import java.time.LocalDateTime;

/**
 * Model representing a user stored in Redis
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@RedisHash("user")
public class User implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    /**
     * Unique ID generated as: tenantId:workspaceId:uniqueUserId
     * Uses uniqueUserId (preferred) or falls back to slackUserId
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
     * Slack user ID (original from Slack API)
     */
    @Indexed
    private String slackUserId;
    
    /**
     * Unique user ID (unified identifier across platforms)
     * Preferred over slackUserId for lookups
     */
    @Indexed
    private String uniqueUserId;
    
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
     * When this user record was first created
     */
    private LocalDateTime createdAt;
    
    /**
     * When this user record was last updated
     */
    private LocalDateTime updatedAt;
    
    /**
     * Last time we saw this user in a message
     */
    private LocalDateTime lastSeenAt;
    
    /**
     * Count of messages from this user
     */
    private Long messageCount;
    
    /**
     * Is this user active in the workspace
     */
    private Boolean isActive;
    
    /**
     * Additional metadata for the user
     */
    private String metadata;
    
    /**
     * Generate composite ID from tenant, workspace, and unique user ID
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @param uniqueUserId The unique user ID (preferred) or slackUserId (fallback)
     * @return Composite ID in format: tenantId:workspaceId:uniqueUserId
     */
    public static String generateId(String tenantId, String workspaceId, String uniqueUserId) {
        if (tenantId == null || workspaceId == null || uniqueUserId == null) {
            throw new IllegalArgumentException("TenantId, workspaceId, and uniqueUserId cannot be null");
        }
        return tenantId + ":" + workspaceId + ":" + uniqueUserId;
    }
    
    /**
     * Update timestamps and activity info
     */
    public void updateActivity() {
        this.updatedAt = LocalDateTime.now();
        this.lastSeenAt = LocalDateTime.now();
        this.messageCount = this.messageCount != null ? this.messageCount + 1 : 1L;
        this.isActive = true;
    }
    
    /**
     * Set creation timestamp if not already set
     */
    public void setCreatedIfNew() {
        if (this.createdAt == null) {
            this.createdAt = LocalDateTime.now();
        }
    }
}
