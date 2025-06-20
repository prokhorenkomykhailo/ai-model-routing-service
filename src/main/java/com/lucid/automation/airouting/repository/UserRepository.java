package com.lucid.automation.airouting.repository;

import com.lucid.automation.airouting.model.User;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Repository interface for User entities in Redis
 */
@Repository
public interface UserRepository extends CrudRepository<User, String> {
    
    /**
     * Find users by tenant ID
     */
    List<User> findByTenantId(String tenantId);
    
    /**
     * Find users by workspace ID
     */
    List<User> findByWorkspaceId(String workspaceId);
    
    /**
     * Find users by tenant ID and workspace ID
     */
    List<User> findByTenantIdAndWorkspaceId(String tenantId, String workspaceId);
    
    /**
     * Find user by slack user ID
     */
    List<User> findBySlackUserId(String slackUserId);
    
    /**
     * Find user by tenant, workspace, and slack user ID (should be unique)
     */
    Optional<User> findByTenantIdAndWorkspaceIdAndSlackUserId(String tenantId, String workspaceId, String slackUserId);
    
    /**
     * Check if user exists by tenant, workspace, and slack user ID
     */
    boolean existsByTenantIdAndWorkspaceIdAndSlackUserId(String tenantId, String workspaceId, String slackUserId);
}
