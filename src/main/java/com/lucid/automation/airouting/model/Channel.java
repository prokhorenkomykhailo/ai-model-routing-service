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
import java.time.Instant;

/**
 * Model representing a channel stored in Redis
 * 
 * @author AI Assistant
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@RedisHash("channel")
public class Channel implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    /**
     * Unique channel identifier
     */
    @Id
    private String channelId;
    
    /**
     * Source of the channel (slack, email, etc.)
     */
    @Indexed
    private String channelSrc;
    
    /**
     * Display name of the channel
     */
    private String channelName;
    
    /**
     * Tenant ID for multi-tenant support
     */
    @Indexed
    private String tenantId;
    
    /**
     * Workspace/Team ID where this channel belongs
     */
    @Indexed
    private String workspaceId;
    
    /**
     * Timestamp when the channel was first created/discovered
     */
    private Long createdAt;
    
    /**
     * Timestamp when the channel information was last updated
     */
    private Long updatedAt;
    
    /**
     * Additional channel metadata
     */
    private String channelType;
    
    /**
     * Whether the channel is private
     */
    private Boolean isPrivate;
    
    /**
     * Channel topic/description
     */
    private String topic;
    
    /**
     * Channel purpose
     */
    private String purpose;
    
    /**
     * Update timestamps before saving
     */
    public void updateTimestamps() {
        final long now = Instant.now().toEpochMilli();
        if (this.createdAt == null) {
            this.createdAt = now;
        }
        this.updatedAt = now;
    }
    
    /**
     * Create a composite key for indexing
     */
    public String getCompositeKey() {
        return String.join(":", 
            tenantId != null ? tenantId : "",
            workspaceId != null ? workspaceId : "",
            channelId != null ? channelId : "");
    }
}
