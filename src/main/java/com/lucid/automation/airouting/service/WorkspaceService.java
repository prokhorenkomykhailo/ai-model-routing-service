package com.lucid.automation.airouting.service;

import com.lucid.automation.slackingestion.dto.messaging.IngestionEventDTO;
import com.lucid.automation.airouting.dto.WorkspaceStats;
import com.lucid.automation.airouting.model.Workspace;
import com.lucid.automation.airouting.repository.WorkspaceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Service for managing workspaces
 */
@Service
public class WorkspaceService {
    
    private static final Logger logger = LoggerFactory.getLogger(WorkspaceService.class);
    
    private final WorkspaceRepository workspaceRepository;
    
    public WorkspaceService(WorkspaceRepository workspaceRepository) {
        this.workspaceRepository = workspaceRepository;
    }
    
    /**
     * Create or update workspace from message data
     * 
     * @param dto The ingestion event DTO containing message data
     * @return The created or updated workspace
     */
    public Workspace createOrUpdateWorkspace(IngestionEventDTO dto) {
        if (dto.getTenantId() == null || dto.getMessage() == null) {
            logger.warn("Cannot create/update workspace: missing tenantId or message data");
            return null;
        }
        
        String workspaceId = dto.getMessage().getTeamId();
        if (workspaceId == null || workspaceId.trim().isEmpty()) {
            logger.warn("Cannot create/update workspace: missing workspaceId (teamId)");
            return null;
        }
        
        String tenantId = dto.getTenantId();
        String tenantSchema = dto.getTenantSchema();
        
        try {
            // Find existing workspace or create new one
            Workspace workspace = workspaceRepository.findByTenantIdAndId(tenantId, workspaceId)
                    .orElse(new Workspace(workspaceId, tenantId, tenantSchema));
            
            // Update workspace with message data
            updateWorkspaceFromMessage(workspace, dto);
            
            // Save and return
            Workspace savedWorkspace = workspaceRepository.save(workspace);
            logger.debug("Updated workspace: {}", savedWorkspace);
            
            return savedWorkspace;
            
        } catch (Exception e) {
            logger.error("Error creating/updating workspace: tenantId={}, workspaceId={}", 
                        tenantId, workspaceId, e);
            return null;
        }
    }
    
    /**
     * Update workspace statistics from message
     * 
     * @param workspace The workspace to update
     * @param dto The message data
     */
    private void updateWorkspaceFromMessage(Workspace workspace, IngestionEventDTO dto) {
        IngestionEventDTO.SlackMessageDTO messageData = dto.getMessage();
        
        // Update basic info
        if (workspace.getTeamId() == null && messageData.getTeamId() != null) {
            workspace.setTeamId(messageData.getTeamId());
        }
        
        // Update tenant schema if not set
        if (workspace.getTenantSchema() == null && dto.getTenantSchema() != null) {
            workspace.setTenantSchema(dto.getTenantSchema());
        }
        
        // Add channel
        if (messageData.getChannelId() != null) {
            workspace.addChannel(messageData.getChannelId());
        }
        
        // Update message count
        workspace.incrementMessageCount();
        
        // Update thread count if this is a thread message
        if (messageData.getThreadTs() != null && !messageData.getThreadTs().isEmpty()) {
            workspace.incrementThreadCount();
        }
        
        // Update message timing
        Instant messageTime = parseMessageTime(dto, messageData);
        workspace.updateLastMessageTime(messageTime);
        
        // Extract workspace name from domain or other metadata if available
        if (workspace.getName() == null) {
            extractWorkspaceName(workspace, dto);
        }
    }
    
    /**
     * Parse message timestamp
     */
    private Instant parseMessageTime(IngestionEventDTO dto, IngestionEventDTO.SlackMessageDTO messageData) {
        // Try to use ingestedAt first
        if (dto.getIngestedAt() != null) {
            return dto.getIngestedAt();
        }
        
        // Try to parse message timestamp
        if (messageData.getTs() != null) {
            try {
                String timestamp = messageData.getTs();
                // Slack timestamps are usually in format "1234567890.123456"
                if (timestamp.contains(".")) {
                    timestamp = timestamp.split("\\.")[0];
                }
                return Instant.ofEpochSecond(Long.parseLong(timestamp));
            } catch (NumberFormatException e) {
                logger.warn("Could not parse message timestamp: {}", messageData.getTs());
            }
        }
        
        // Fallback to current time
        return Instant.now();
    }
    
    /**
     * Extract workspace name from message metadata
     */
    private void extractWorkspaceName(Workspace workspace, IngestionEventDTO dto) {
        // Try to get name from metadata
        if (dto.getMetadata() != null && dto.getMetadata().getWorkspaceName() != null) {
            workspace.setName(dto.getMetadata().getWorkspaceName());
        } else {
            // Default name
            workspace.setName("Workspace " + workspace.getId());
        }
    }
    
    /**
     * Get all workspaces
     * 
     * @return List of all workspaces
     */
    public List<Workspace> getAllWorkspaces() {
        try {
            List<Workspace> workspaces = new ArrayList<>();
            workspaceRepository.findAll().forEach(workspaces::add);
            logger.debug("Retrieved {} workspaces", workspaces.size());
            return workspaces;
        } catch (Exception e) {
            logger.error("Error retrieving all workspaces", e);
            return Collections.emptyList();
        }
    }
    
    /**
     * Get workspaces by tenant
     * 
     * @param tenantId The tenant ID
     * @return List of workspaces for the tenant
     */
    public List<Workspace> getWorkspacesByTenant(String tenantId) {
        if (tenantId == null || tenantId.trim().isEmpty()) {
            logger.warn("getWorkspacesByTenant called with null or empty tenantId");
            return Collections.emptyList();
        }
        
        try {
            List<Workspace> workspaces = workspaceRepository.findByTenantId(tenantId);
            logger.debug("Retrieved {} workspaces for tenant: {}", workspaces.size(), tenantId);
            return workspaces;
        } catch (Exception e) {
            logger.error("Error retrieving workspaces for tenant: {}", tenantId, e);
            return Collections.emptyList();
        }
    }
    
    /**
     * Get workspaces by tenant schema
     * 
     * @param tenantSchema The tenant schema
     * @return List of workspaces
     */
    public List<Workspace> getWorkspacesByTenantSchema(String tenantSchema) {
        if (tenantSchema == null || tenantSchema.trim().isEmpty()) {
            logger.warn("getWorkspacesByTenantSchema called with null or empty tenantSchema");
            return Collections.emptyList();
        }
        
        try {
            List<Workspace> workspaces = workspaceRepository.findByTenantSchema(tenantSchema);
            logger.debug("Retrieved {} workspaces for tenant schema: {}", workspaces.size(), tenantSchema);
            return workspaces;
        } catch (Exception e) {
            logger.error("Error retrieving workspaces for tenant schema: {}", tenantSchema, e);
            return Collections.emptyList();
        }
    }
    
    /**
     * Get workspaces by tenant and tenant schema
     * 
     * @param tenantId The tenant ID
     * @param tenantSchema The tenant schema
     * @return List of workspaces
     */
    public List<Workspace> getWorkspacesByTenantAndSchema(String tenantId, String tenantSchema) {
        if (tenantId == null || tenantSchema == null) {
            logger.warn("getWorkspacesByTenantAndSchema called with null parameters");
            return Collections.emptyList();
        }
        
        try {
            List<Workspace> workspaces = workspaceRepository.findByTenantIdAndTenantSchema(tenantId, tenantSchema);
            logger.debug("Retrieved {} workspaces for tenant: {} and schema: {}", workspaces.size(), tenantId, tenantSchema);
            return workspaces;
        } catch (Exception e) {
            logger.error("Error retrieving workspaces for tenant: {} and schema: {}", tenantId, tenantSchema, e);
            return Collections.emptyList();
        }
    }
    
    /**
     * Get workspace by ID
     * 
     * @param workspaceId The workspace ID
     * @return Optional workspace
     */
    public Optional<Workspace> getWorkspaceById(String workspaceId) {
        if (workspaceId == null || workspaceId.trim().isEmpty()) {
            return Optional.empty();
        }
        
        try {
            return workspaceRepository.findById(workspaceId);
        } catch (Exception e) {
            logger.error("Error retrieving workspace: {}", workspaceId, e);
            return Optional.empty();
        }
    }
    
    /**
     * Get workspace by tenant and workspace ID
     * 
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @return Optional workspace
     */
    public Optional<Workspace> getWorkspace(String tenantId, String workspaceId) {
        if (tenantId == null || workspaceId == null) {
            return Optional.empty();
        }
        
        try {
            return workspaceRepository.findByTenantIdAndId(tenantId, workspaceId);
        } catch (Exception e) {
            logger.error("Error retrieving workspace: tenantId={}, workspaceId={}", tenantId, workspaceId, e);
            return Optional.empty();
        }
    }
    
    /**
     * Get workspace by tenant, tenant schema and workspace ID
     * 
     * @param tenantId The tenant ID
     * @param tenantSchema The tenant schema  
     * @param workspaceId The workspace ID
     * @return Optional workspace
     */
    public Optional<Workspace> getWorkspace(String tenantId, String tenantSchema, String workspaceId) {
        if (tenantId == null || tenantSchema == null || workspaceId == null) {
            return Optional.empty();
        }
        
        try {
            return workspaceRepository.findByTenantIdAndTenantSchemaAndId(tenantId, tenantSchema, workspaceId);
        } catch (Exception e) {
            logger.error("Error retrieving workspace: tenantId={}, tenantSchema={}, workspaceId={}", 
                        tenantId, tenantSchema, workspaceId, e);
            return Optional.empty();
        }
    }

    /**
     * Get workspace statistics
     * 
     * @param workspaceId The workspace ID
     * @return Workspace statistics or null if not found
     */
    public WorkspaceStats getWorkspaceStats(String workspaceId) {
        return workspaceRepository.findById(workspaceId)
                .map(workspace -> new WorkspaceStats(
                        workspace.getId(),
                        workspace.getTenantId(),
                        workspace.getTenantSchema(),
                        workspace.getName(),
                        workspace.getTotalMessages(),
                        workspace.getTotalChannels(),
                        workspace.getTotalThreads(),
                        workspace.getFirstMessageAt(),
                        workspace.getLastMessageAt(),
                        workspace.getChannelIds()
                ))
                .orElse(null);
    }
    
    /**
     * Delete workspace
     * 
     * @param workspaceId The workspace ID to delete
     * @return true if deleted successfully, false otherwise
     */
    public boolean deleteWorkspace(String workspaceId) {
        if (workspaceId == null || workspaceId.trim().isEmpty()) {
            return false;
        }
        
        try {
            if (workspaceRepository.existsById(workspaceId)) {
                workspaceRepository.deleteById(workspaceId);
                logger.info("Deleted workspace: {}", workspaceId);
                return true;
            } else {
                logger.warn("Workspace not found for deletion: {}", workspaceId);
                return false;
            }
        } catch (Exception e) {
            logger.error("Error deleting workspace: {}", workspaceId, e);
            return false;
        }
    }
    
    /**
     * Get recent workspaces (active in last N days)
     * 
     * @param days Number of days to look back
     * @return List of workspaces active in the specified period
     */
    public List<Workspace> getRecentWorkspaces(int days) {
        try {
            Instant since = Instant.now().minus(Duration.ofDays(days));
            List<Workspace> workspaces = workspaceRepository.findByLastMessageAtAfter(since);
            logger.debug("Retrieved {} recent workspaces (last {} days)", workspaces.size(), days);
            return workspaces;
        } catch (Exception e) {
            logger.error("Error retrieving recent workspaces", e);
            return Collections.emptyList();
        }
    }
    
    /**
     * Search workspaces by name
     * 
     * @param name The name pattern to search for
     * @return List of matching workspaces
     */
    public List<Workspace> searchWorkspacesByName(String name) {
        if (name == null || name.trim().isEmpty()) {
            return Collections.emptyList();
        }
        
        try {
            List<Workspace> workspaces = workspaceRepository.findByNameContainingIgnoreCase(name.trim());
            logger.debug("Found {} workspaces matching name: {}", workspaces.size(), name);
            return workspaces;
        } catch (Exception e) {
            logger.error("Error searching workspaces by name: {}", name, e);
            return Collections.emptyList();
        }
    }
    
    /**
     * Count workspaces by tenant
     * 
     * @param tenantId The tenant ID
     * @return Number of workspaces for the tenant
     */
    public long countWorkspacesByTenant(String tenantId) {
        if (tenantId == null || tenantId.trim().isEmpty()) {
            return 0;
        }
        
        try {
            return workspaceRepository.countByTenantId(tenantId);
        } catch (Exception e) {
            logger.error("Error counting workspaces for tenant: {}", tenantId, e);
            return 0;
        }
    }
}
