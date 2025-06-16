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
 * Model representing a conversation message stored in Redis
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@RedisHash("conversation_message")
public class ConversationMessage implements Serializable {
    
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
}
