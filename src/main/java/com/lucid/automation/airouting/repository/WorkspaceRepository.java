package com.lucid.automation.airouting.repository;

import com.lucid.automation.airouting.model.Workspace;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Repository interface for Workspace Redis operations
 */
@Repository
public interface WorkspaceRepository extends CrudRepository<Workspace, String> {
    
    /**
     * Find all workspaces for a tenant
     * 
     * @param tenantId The tenant ID
     * @return List of workspaces
     */
    List<Workspace> findByTenantId(String tenantId);
    
    /**
     * Find workspace by tenant and workspace ID
     * 
     * @param tenantId The tenant ID
     * @param id The workspace ID
     * @return Optional workspace
     */
    Optional<Workspace> findByTenantIdAndId(String tenantId, String id);
    
    /**
     * Find workspaces by team ID (Slack team)
     * 
     * @param teamId The Slack team ID
     * @return List of workspaces
     */
    List<Workspace> findByTeamId(String teamId);
    
    /**
     * Find workspaces with messages after a certain date
     * 
     * @param since The timestamp to search from
     * @return List of workspaces
     */
    List<Workspace> findByLastMessageAtAfter(Instant since);
    
    /**
     * Find workspaces created after a certain date
     * 
     * @param since The timestamp to search from
     * @return List of workspaces
     */
    List<Workspace> findByCreatedAtAfter(Instant since);
    
    /**
     * Count workspaces by tenant
     * 
     * @param tenantId The tenant ID
     * @return Number of workspaces
     */
    long countByTenantId(String tenantId);
    
    /**
     * Find workspaces by tenant schema
     * 
     * @param tenantSchema The tenant schema
     * @return List of workspaces
     */
    List<Workspace> findByTenantSchema(String tenantSchema);
    
    /**
     * Find workspace by tenant, tenant schema and workspace ID
     * 
     * @param tenantId The tenant ID
     * @param tenantSchema The tenant schema
     * @param id The workspace ID
     * @return Optional workspace
     */
    Optional<Workspace> findByTenantIdAndTenantSchemaAndId(String tenantId, String tenantSchema, String id);
    
    /**
     * Find workspaces by tenant and tenant schema
     * 
     * @param tenantId The tenant ID
     * @param tenantSchema The tenant schema
     * @return List of workspaces
     */
    List<Workspace> findByTenantIdAndTenantSchema(String tenantId, String tenantSchema);
    
    /**
     * Find workspaces by name containing (case insensitive)
     * 
     * @param name The name pattern to search for
     * @return List of workspaces
     */
    List<Workspace> findByNameContainingIgnoreCase(String name);
    
    /**
     * Find workspaces updated after a certain date
     * 
     * @param since The timestamp to search from
     * @return List of workspaces
     */
    List<Workspace> findByUpdatedAtAfter(Instant since);
}
