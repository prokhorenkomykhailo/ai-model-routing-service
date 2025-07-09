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
     * Find user by slack user ID (used by SlidingWindowService)
     */
    List<User> findBySlackUserId(String slackUserId);
    
    /**
     * Find user by tenant, workspace, and slack user ID (used by consumers)
     */
    Optional<User> findByTenantIdAndWorkspaceIdAndSlackUserId(String tenantId, String workspaceId, String slackUserId);
}
