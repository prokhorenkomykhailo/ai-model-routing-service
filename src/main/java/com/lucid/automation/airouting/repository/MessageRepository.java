package com.lucid.automation.airouting.repository;

import com.lucid.automation.airouting.model.Message;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Repository interface for Message Redis operations
 */
@Repository
public interface MessageRepository extends CrudRepository<Message, String> {
    
    /**
     * Find all messages by tenant ID
     * 
     * @param tenantId The tenant ID
     * @return List of messages
     */
    List<Message> findByTenantId(String tenantId);
    
    /**
     * Find all messages by workspace ID
     * 
     * @param workspaceId The workspace ID
     * @return List of messages
     */
    List<Message> findByWorkspaceId(String workspaceId);
    
    /**
     * Find all messages by tenant ID and workspace ID
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @return List of messages
     */
    List<Message> findByTenantIdAndWorkspaceId(String tenantId, String workspaceId);
    
    /**
     * Find all messages by tenant ID, workspace ID, and channel ID
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @return List of messages
     */
    List<Message> findByTenantIdAndWorkspaceIdAndChannelId(
            String tenantId, String workspaceId, String channelId);
    
    /**
     * Find all messages by tenant ID, workspace ID, channel ID, and thread timestamp
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @param threadTs The thread timestamp
     * @return List of messages
     */
    List<Message> findByTenantIdAndWorkspaceIdAndChannelIdAndThreadTs(
            String tenantId, String workspaceId, String channelId, String threadTs);
    
    /**
     * Count messages by tenant ID
     * 
     * @param tenantId The tenant ID
     * @return Number of messages
     */
    long countByTenantId(String tenantId);
    
    /**
     * Count messages by workspace ID
     * 
     * @param workspaceId The workspace ID
     * @return Number of messages
     */
    long countByWorkspaceId(String workspaceId);
    
    /**
     * Count messages by tenant ID and workspace ID
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @return Number of messages
     */
    long countByTenantIdAndWorkspaceId(String tenantId, String workspaceId);
    
    /**
     * Count messages by tenant ID, workspace ID, and channel ID
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @return Number of messages
     */
    long countByTenantIdAndWorkspaceIdAndChannelId(
            String tenantId, String workspaceId, String channelId);
    
    /**
     * Count messages by tenant ID, workspace ID, channel ID, and thread timestamp
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @param threadTs The thread timestamp
     * @return Number of messages
     */
    long countByTenantIdAndWorkspaceIdAndChannelIdAndThreadTs(
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
    
    /**
     * Delete all messages by tenant ID
     * 
     * @param tenantId The tenant ID
     */
    void deleteByTenantId(String tenantId);
    
    /**
     * Delete all messages by workspace ID
     * 
     * @param workspaceId The workspace ID
     */
    void deleteByWorkspaceId(String workspaceId);
    
    /**
     * Delete all messages by tenant ID and workspace ID
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     */
    void deleteByTenantIdAndWorkspaceId(String tenantId, String workspaceId);
    
    /**
     * Find all messages by workspace ID with pagination
     * 
     * @param workspaceId The workspace ID
     * @param pageable Pagination information
     * @return List of messages limited by pagination
     */
    List<Message> findByWorkspaceId(String workspaceId, Pageable pageable);
    
    /**
     * Find all messages by tenant ID with pagination
     * 
     * @param tenantId The tenant ID
     * @param pageable Pagination information
     * @return List of messages limited by pagination
     */
    List<Message> findByTenantId(String tenantId, Pageable pageable);
    
    /**
     * Find all messages by tenant ID and workspace ID with pagination
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @param pageable Pagination information
     * @return List of messages limited by pagination
     */
    List<Message> findByTenantIdAndWorkspaceId(String tenantId, String workspaceId, Pageable pageable);
    
    /**
     * Find all messages by tenant ID, workspace ID, and channel ID with pagination
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @param pageable Pagination information
     * @return List of messages limited by pagination
     */
    List<Message> findByTenantIdAndWorkspaceIdAndChannelId(
            String tenantId, String workspaceId, String channelId, Pageable pageable);
    
    /**
     * Find all messages by workspace ID and channel ID
     * 
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @return List of messages
     */
    List<Message> findByWorkspaceIdAndChannelId(String workspaceId, String channelId);
    
    /**
     * Find all messages by workspace ID, channel ID, and thread timestamp
     * 
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @param threadTs The thread timestamp
     * @return List of messages
     */
    List<Message> findByWorkspaceIdAndChannelIdAndThreadTs(String workspaceId, String channelId, String threadTs);
    
    /**
     * Find paginated messages by workspace ID and channel ID
     * 
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @param pageable Pagination information
     * @return Page of messages
     */
    Page<Message> findByWorkspaceIdAndChannelId(String workspaceId, String channelId, Pageable pageable);
    
    /**
     * Find paginated messages by workspace ID, channel ID, and thread timestamp
     * 
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @param threadTs The thread timestamp
     * @param pageable Pagination information
     * @return Page of messages
     */
    Page<Message> findByWorkspaceIdAndChannelIdAndThreadTs(String workspaceId, String channelId, String threadTs, Pageable pageable);
    
    /**
     * Count messages by workspace ID and channel ID
     * 
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @return Number of messages
     */
    long countByWorkspaceIdAndChannelId(String workspaceId, String channelId);
    
    /**
     * Count messages by workspace ID, channel ID, and thread timestamp
     * 
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @param threadTs The thread timestamp
     * @return Number of messages
     */
    long countByWorkspaceIdAndChannelIdAndThreadTs(String workspaceId, String channelId, String threadTs);
    
    /**
     * Delete messages by workspace ID and channel ID
     * 
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     */
    void deleteByWorkspaceIdAndChannelId(String workspaceId, String channelId);
    
    /**
     * Delete messages by workspace ID, channel ID, and thread timestamp
     * 
     * @param workspaceId The workspace ID
     * @param channelId The channel ID
     * @param threadTs The thread timestamp
     */
    void deleteByWorkspaceIdAndChannelIdAndThreadTs(String workspaceId, String channelId, String threadTs);
    
    /**
     * Find all messages by channel ID
     * 
     * @param channelId The channel ID
     * @return List of messages
     */
    List<Message> findByChannelId(String channelId);
    
    /**
     * Find all messages by thread timestamp
     * 
     * @param threadTs The thread timestamp
     * @return List of messages
     */
    List<Message> findByThreadTs(String threadTs);

    // Optimized methods using composite indexes
    
    /**
     * Find messages using composite tenant-workspace index (optimized)
     * 
     * @param tenantWorkspaceIndex The composite index value "tenantId:workspaceId"
     * @return List of messages
     */
    List<Message> findByTenantWorkspaceIndex(String tenantWorkspaceIndex);
    
    /**
     * Find messages using composite tenant-workspace-channel index (optimized)
     * 
     * @param tenantWorkspaceChannelIndex The composite index value "tenantId:workspaceId:channelId"
     * @return List of messages
     */
    List<Message> findByTenantWorkspaceChannelIndex(String tenantWorkspaceChannelIndex);
    
    /**
     * Find messages using composite tenant-workspace-channel-thread index (optimized)
     * 
     * @param tenantWorkspaceChannelThreadIndex The composite index value "tenantId:workspaceId:channelId:threadTs"
     * @return List of messages
     */
    List<Message> findByTenantWorkspaceChannelThreadIndex(String tenantWorkspaceChannelThreadIndex);
    
    /**
     * Find messages using composite workspace-channel-thread index (optimized)
     * 
     * @param workspaceChannelThreadIndex The composite index value "workspaceId:channelId:threadTs"
     * @return List of messages
     */
    List<Message> findByWorkspaceChannelThreadIndex(String workspaceChannelThreadIndex);
    
    /**
     * Find messages using composite tenant-workspace index with pagination (optimized)
     * 
     * @param tenantWorkspaceIndex The composite index value "tenantId:workspaceId"
     * @param pageable Pagination information
     * @return Page of messages
     */
    Page<Message> findByTenantWorkspaceIndex(String tenantWorkspaceIndex, Pageable pageable);
    
    /**
     * Find messages using composite workspace-channel-thread index with pagination (optimized)
     * 
     * @param workspaceChannelThreadIndex The composite index value "workspaceId:channelId:threadTs"
     * @param pageable Pagination information
     * @return Page of messages
     */
    Page<Message> findByWorkspaceChannelThreadIndex(String workspaceChannelThreadIndex, Pageable pageable);
    
    /**
     * Count messages using composite tenant-workspace index (optimized)
     * 
     * @param tenantWorkspaceIndex The composite index value "tenantId:workspaceId"
     * @return Number of messages
     */
    long countByTenantWorkspaceIndex(String tenantWorkspaceIndex);
    
    /**
     * Count messages using composite workspace-channel-thread index (optimized)
     * 
     * @param workspaceChannelThreadIndex The composite index value "workspaceId:channelId:threadTs"
     * @return Number of messages
     */
    long countByWorkspaceChannelThreadIndex(String workspaceChannelThreadIndex);
}
