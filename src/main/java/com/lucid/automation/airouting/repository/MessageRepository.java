package com.lucid.automation.airouting.repository;

import com.lucid.automation.airouting.model.Message;
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
     * Find all messages by tenant ID with pagination
     *
     * @param tenantId The tenant ID
     * @param pageable Pagination information
     * @return List of messages limited by pagination
     */
    List<Message> findByTenantId(String tenantId, Pageable pageable);

    List<Message> findAllByDeemergeUserId(String deemergeUserId);

    /**
     * Find all messages by tenant ID and channel ID
     *
     * @param tenantId The tenant ID
     * @param deemergeUserId The deemerge user ID
     * @return List of messages
     */
    List<Message> findByTenantIdAndDeemergeUserId(String tenantId, String deemergeUserId);

    /**
     * Find active (non-deleted) messages by tenant ID and deemerge user ID
     * Excludes soft-deleted messages from processing
     *
     * @param tenantId The tenant ID
     * @param deemergeUserId The deemerge user ID
     * @param isDeleted Deleted status (should be false or null)
     * @return List of active messages
     */
    List<Message> findByTenantIdAndDeemergeUserIdAndIsDeleted(String tenantId, String deemergeUserId, Boolean isDeleted);

    /**
     * Find active (non-deleted) messages by tenant ID and deemerge user ID
     * This is a convenience method that automatically filters out deleted messages
     *
     * @param tenantId The tenant ID
     * @param deemergeUserId The deemerge user ID
     * @return List of active messages
     */
    default List<Message> findActiveMessagesByTenantIdAndDeemergeUserId(String tenantId, String deemergeUserId) {
        return findByTenantIdAndDeemergeUserIdAndIsDeleted(tenantId, deemergeUserId, false);
    }

    /**
     * Find unprocessed messages by workspace ID ordered by message timestamp
     * Used by token-based sliding window processing
     *
     * @param workspaceId The workspace ID
     * @param isProcessed Processing status flag
     * @return List of messages ordered by messageTs ascending
     */
    List<Message> findByWorkspaceIdAndIsProcessedOrderByMessageTsAsc(String workspaceId, Boolean isProcessed);

    /**
     * Find active (non-deleted) unprocessed messages by workspace ID
     * Excludes soft-deleted messages from processing pipeline
     *
     * @param workspaceId The workspace ID
     * @param isProcessed Processing status flag (typically false)
     * @param isDeleted Deleted status (should be false or null)
     * @return List of active messages ordered by messageTs ascending
     */
    List<Message> findByWorkspaceIdAndIsProcessedAndIsDeletedOrderByMessageTsAsc(
            String workspaceId, Boolean isProcessed, Boolean isDeleted);

    /**
     * Find active unprocessed messages for workspace
     * Convenience method that automatically filters out deleted messages
     *
     * @param workspaceId The workspace ID
     * @return List of active unprocessed messages ordered by messageTs
     */
    default List<Message> findActiveUnprocessedMessagesByWorkspace(String workspaceId) {
        return findByWorkspaceIdAndIsProcessedAndIsDeletedOrderByMessageTsAsc(workspaceId, false, false);
    }

    /**
     * Find all messages by tenant ID (for aggregation)
     * NOTE: Use with caution - can return large result sets
     *
     * @param tenantId The tenant ID
     * @return List of all messages for the tenant
     */
    List<Message> findAllByTenantId(String tenantId);

    /**
     * Count messages by tenant ID
     * Uses Redis secondary index on tenantId
     *
     * @param tenantId The tenant ID
     * @return Count of messages for the tenant
     */
    long countByTenantId(String tenantId);

    // ==================== Soft Delete Queries ====================

    /**
     * Find non-deleted messages by tenant ID with pagination
     * Excludes messages where isDeleted=true
     *
     * @param tenantId The tenant ID
     * @param isDeleted Deleted status (false or null)
     * @param pageable Pagination information
     * @return List of non-deleted messages
     */
    List<Message> findByTenantIdAndIsDeleted(String tenantId, Boolean isDeleted, Pageable pageable);

    /**
     * Find messages by tenant ID where isDeleted is false or null
     * This is the primary query for retrieving active (non-deleted) messages
     *
     * @param tenantId The tenant ID
     * @param pageable Pagination information
     * @return List of active messages
     */
    default List<Message> findActiveMessagesByTenantId(String tenantId, Pageable pageable) {
        return findByTenantIdAndIsDeleted(tenantId, false, pageable);
    }

    /**
     * Find soft-deleted messages by tenant ID (for admin view)
     *
     * @param tenantId The tenant ID
     * @param isDeleted Deleted status (true)
     * @param pageable Pagination information
     * @return List of deleted messages
     */
    default List<Message> findDeletedMessagesByTenantId(String tenantId, Pageable pageable) {
        return findByTenantIdAndIsDeleted(tenantId, true, pageable);
    }

    /**
     * Find expired messages eligible for hard delete
     * Used by retention cleanup job to find messages past retention period
     *
     * @param isDeleted Must be true (only deleted messages)
     * @param retentionExpiry Maximum expiry timestamp
     * @param pageable Pagination for batch processing
     * @return List of expired messages ready for physical deletion
     */
    List<Message> findByIsDeletedAndRetentionExpiryLessThan(Boolean isDeleted, Long retentionExpiry, Pageable pageable);

    /**
     * Count soft-deleted messages
     * Used for monitoring and statistics
     *
     * @param isDeleted Deleted status (true)
     * @return Count of soft-deleted messages
     */
    long countByIsDeleted(Boolean isDeleted);

}
