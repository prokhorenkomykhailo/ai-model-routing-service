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
        // Get all messages and filter out soft-deleted ones (isDeleted=true)
        // This keeps both isDeleted=false and isDeleted=null messages
        List<Message> allMessages = findByTenantIdAndDeemergeUserId(tenantId, deemergeUserId);
        return allMessages.stream()
                .filter(msg -> msg.getIsDeleted() == null || !msg.getIsDeleted())
                .toList();
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
        // Get all unprocessed messages and filter out soft-deleted ones (isDeleted=true)
        // This keeps both isDeleted=false and isDeleted=null messages
        List<Message> allMessages = findByWorkspaceIdAndIsProcessedOrderByMessageTsAsc(workspaceId, false);
        return allMessages.stream()
                .filter(msg -> msg.getIsDeleted() == null || !msg.getIsDeleted())
                .sorted((m1, m2) -> {
                    if (m1.getMessageTs() == null && m2.getMessageTs() == null) return 0;
                    if (m1.getMessageTs() == null) return 1;
                    if (m2.getMessageTs() == null) return -1;
                    return m1.getMessageTs().compareTo(m2.getMessageTs());
                })
                .toList();
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
     * Note: Due to Redis secondary index limitations, this fetches ALL messages for the tenant
     * and then filters. For large datasets, consider using a separate index or data structure.
     *
     * @param tenantId The tenant ID
     * @param pageable Pagination information
     * @return List of active messages
     */
    default List<Message> findActiveMessagesByTenantId(String tenantId, Pageable pageable) {
        // Fetch ALL messages for tenant (without pagination)
        List<Message> allMessages = findAllByTenantId(tenantId);

        // Filter to keep only non-deleted (isDeleted==null or isDeleted==false)
        List<Message> activeMessages = allMessages.stream()
                .filter(msg -> msg.getIsDeleted() == null || !msg.getIsDeleted())
                .toList();

        // Apply manual pagination
        int start = (int) pageable.getOffset();
        int end = Math.min(start + pageable.getPageSize(), activeMessages.size());

        if (start >= activeMessages.size()) {
            return List.of();
        }

        return activeMessages.subList(start, end);
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
