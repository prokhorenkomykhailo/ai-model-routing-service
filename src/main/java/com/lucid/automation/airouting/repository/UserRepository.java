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
     * Find user by unique user ID (preferred method for lookups)
     */
    List<User> findByUniqueUserId(String uniqueUserId);
    
    /**
     * Find user by tenant, workspace, and unique user ID (primary lookup method)
     */
    Optional<User> findByTenantIdAndWorkspaceIdAndUniqueUserId(String tenantId, String workspaceId, String uniqueUserId);
    
    /**
     * Find user by slack user ID (legacy support)
     * @deprecated Use findByUniqueUserId instead
     */
    @Deprecated
    List<User> findBySlackUserId(String slackUserId);
    
    /**
     * Find user by tenant, workspace, and slack user ID (legacy support)
     * @deprecated Use findByTenantIdAndWorkspaceIdAndUniqueUserId instead
     */
    @Deprecated
    Optional<User> findByTenantIdAndWorkspaceIdAndSlackUserId(String tenantId, String workspaceId, String slackUserId);
}
