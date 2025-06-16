package com.lucid.automation.airouting.repository;

import com.lucid.automation.airouting.model.ConversationMessage;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Repository interface for ConversationMessage Redis operations
 */
@Repository
public interface ConversationMessageRepository extends CrudRepository<ConversationMessage, String> {
    
    /**
     * Find all messages by tenant ID
     * 
     * @param tenantId The tenant ID
     * @return List of conversation messages
     */
    List<ConversationMessage> findByTenantId(String tenantId);
    
    /**
     * Find all messages by tenant ID and workspace ID
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @return List of conversation messages
     */
    List<ConversationMessage> findByTenantIdAndWorkspaceId(String tenantId, String workspaceId);
    
    /**
     * Find all messages by tenant ID, workspace ID, and channel ID
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @return List of conversation messages
     */
    List<ConversationMessage> findByTenantIdAndWorkspaceIdAndChannelId(
            String tenantId, String workspaceId, String channelId);
    
    /**
     * Find all messages by tenant ID, workspace ID, channel ID, and thread timestamp
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @param threadTs The thread timestamp
     * @return List of conversation messages
     */
    List<ConversationMessage> findByTenantIdAndWorkspaceIdAndChannelIdAndThreadTs(
            String tenantId, String workspaceId, String channelId, String threadTs);
    
    /**
     * Delete all messages by tenant ID, workspace ID, channel ID, and thread timestamp
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @param threadTs The thread timestamp
     */
    void deleteByTenantIdAndWorkspaceIdAndChannelIdAndThreadTs(
            String tenantId, String workspaceId, String channelId, String threadTs);
}
