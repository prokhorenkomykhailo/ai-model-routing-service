package com.lucid.automation.airouting.repository;

import com.lucid.automation.airouting.model.Channel;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Repository for Channel entities in Redis
 * 
 * @author AI Assistant
 */
@Repository
public interface ChannelRepository extends CrudRepository<Channel, String> {
    
    /**
     * Find channels by source (slack, email, etc.)
     * 
     * @param channelSrc The channel source
     * @return List of channels from the specified source
     */
    List<Channel> findByChannelSrc(String channelSrc);
    
    /**
     * Find channels by tenant ID
     * 
     * @param tenantId The tenant ID
     * @return List of channels for the tenant
     */
    List<Channel> findByTenantId(String tenantId);
    
    /**
     * Find channels by workspace ID
     * 
     * @param workspaceId The workspace/team ID
     * @return List of channels in the workspace
     */
    List<Channel> findByWorkspaceId(String workspaceId);
    
    /**
     * Find channels by tenant and workspace
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @return List of channels for the tenant and workspace
     */
    List<Channel> findByTenantIdAndWorkspaceId(String tenantId, String workspaceId);
    
    /**
     * Find channel by exact channel ID
     * 
     * @param channelId The channel ID
     * @return Optional containing the channel if found
     */
    Optional<Channel> findByChannelId(String channelId);
    
    /**
     * Find channels by name (case-insensitive)
     * 
     * @param channelName The channel name
     * @return List of channels with matching name
     */
    List<Channel> findByChannelNameIgnoreCase(String channelName);
    
    /**
     * Check if a channel exists by ID
     * 
     * @param channelId The channel ID
     * @return true if channel exists, false otherwise
     */
    boolean existsByChannelId(String channelId);
}
