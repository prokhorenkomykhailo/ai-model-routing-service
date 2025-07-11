package com.lucid.automation.airouting.repository;

import com.lucid.automation.airouting.model.Workspace;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Repository interface for Workspace Redis operations
 */
@Repository
public interface WorkspaceRepository extends CrudRepository<Workspace, String> {

    Optional<Workspace> findByTeamIdAndTenantIdAndDeemergeUserId(String teamId, String tenantId, String deemergeUserId);
}
