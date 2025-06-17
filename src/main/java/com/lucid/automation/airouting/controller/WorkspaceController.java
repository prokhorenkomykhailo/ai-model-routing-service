package com.lucid.automation.airouting.controller;

import com.lucid.automation.airouting.dto.WorkspaceStats;
import com.lucid.automation.airouting.model.Workspace;
import com.lucid.automation.airouting.service.WorkspaceService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

/**
 * REST Controller for workspace management
 */
@RestController
@RequestMapping("/api/workspaces")
@CrossOrigin(origins = "*")
@Tag(name = "Workspace Management", description = "Operations for managing workspaces")
public class WorkspaceController {
    
    private static final Logger logger = LoggerFactory.getLogger(WorkspaceController.class);
    
    private final WorkspaceService workspaceService;
    
    public WorkspaceController(WorkspaceService workspaceService) {
        this.workspaceService = workspaceService;
    }
    
    /**
     * Get all workspaces
     */
    @GetMapping
    @Operation(summary = "Get all workspaces", description = "Retrieves all workspaces in the system")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Workspaces retrieved successfully")
    })
    public ResponseEntity<List<Workspace>> getAllWorkspaces() {
        logger.debug("Getting all workspaces");
        
        try {
            List<Workspace> workspaces = workspaceService.getAllWorkspaces();
            logger.info("Retrieved {} workspaces", workspaces.size());
            return ResponseEntity.ok(workspaces);
        } catch (Exception e) {
            logger.error("Error retrieving all workspaces", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Get workspaces by tenant
     */
    @GetMapping("/tenant/{tenantId}")
    @Operation(summary = "Get workspaces by tenant", description = "Retrieves all workspaces for a specific tenant")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Workspaces retrieved successfully"),
        @ApiResponse(responseCode = "400", description = "Invalid tenant ID")
    })
    public ResponseEntity<List<Workspace>> getWorkspacesByTenant(
            @Parameter(description = "Tenant ID") @PathVariable String tenantId) {
        
        logger.debug("Getting workspaces for tenant: {}", tenantId);
        
        if (tenantId == null || tenantId.trim().isEmpty()) {
            return ResponseEntity.badRequest().build();
        }
        
        try {
            List<Workspace> workspaces = workspaceService.getWorkspacesByTenant(tenantId);
            logger.info("Retrieved {} workspaces for tenant: {}", workspaces.size(), tenantId);
            return ResponseEntity.ok(workspaces);
        } catch (Exception e) {
            logger.error("Error retrieving workspaces for tenant: {}", tenantId, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Get workspace by ID
     */
    @GetMapping("/{workspaceId}")
    @Operation(summary = "Get workspace by ID", description = "Retrieves a specific workspace by its ID")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Workspace found"),
        @ApiResponse(responseCode = "404", description = "Workspace not found")
    })
    public ResponseEntity<Workspace> getWorkspace(
            @Parameter(description = "Workspace ID") @PathVariable String workspaceId) {
        
        logger.debug("Getting workspace: {}", workspaceId);
        
        try {
            Optional<Workspace> workspace = workspaceService.getWorkspaceById(workspaceId);
            
            if (workspace.isPresent()) {
                return ResponseEntity.ok(workspace.get());
            } else {
                logger.warn("Workspace not found: {}", workspaceId);
                return ResponseEntity.notFound().build();
            }
        } catch (Exception e) {
            logger.error("Error retrieving workspace: {}", workspaceId, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Get workspace by tenant and workspace ID
     */
    @GetMapping("/tenant/{tenantId}/workspace/{workspaceId}")
    @Operation(summary = "Get workspace by tenant and ID", description = "Retrieves a workspace by tenant and workspace ID")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Workspace found"),
        @ApiResponse(responseCode = "404", description = "Workspace not found")
    })
    public ResponseEntity<Workspace> getWorkspaceByTenantAndId(
            @Parameter(description = "Tenant ID") @PathVariable String tenantId,
            @Parameter(description = "Workspace ID") @PathVariable String workspaceId) {
        
        logger.debug("Getting workspace: {} for tenant: {}", workspaceId, tenantId);
        
        try {
            Optional<Workspace> workspace = workspaceService.getWorkspace(tenantId, workspaceId);
            
            if (workspace.isPresent()) {
                return ResponseEntity.ok(workspace.get());
            } else {
                logger.warn("Workspace not found: {} for tenant: {}", workspaceId, tenantId);
                return ResponseEntity.notFound().build();
            }
        } catch (Exception e) {
            logger.error("Error retrieving workspace: {} for tenant: {}", workspaceId, tenantId, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Get workspace statistics
     */
    @GetMapping("/{workspaceId}/stats")
    @Operation(summary = "Get workspace statistics", description = "Retrieves detailed statistics for a workspace")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Statistics retrieved successfully"),
        @ApiResponse(responseCode = "404", description = "Workspace not found")
    })
    public ResponseEntity<WorkspaceStats> getWorkspaceStats(
            @Parameter(description = "Workspace ID") @PathVariable String workspaceId) {
        
        logger.debug("Getting statistics for workspace: {}", workspaceId);
        
        try {
            WorkspaceStats stats = workspaceService.getWorkspaceStats(workspaceId);
            
            if (stats != null) {
                return ResponseEntity.ok(stats);
            } else {
                logger.warn("Workspace not found for stats: {}", workspaceId);
                return ResponseEntity.notFound().build();
            }
        } catch (Exception e) {
            logger.error("Error retrieving workspace stats: {}", workspaceId, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Get recent workspaces (active in last N days)
     */
    @GetMapping("/recent")
    @Operation(summary = "Get recent workspaces", description = "Retrieves workspaces that were active in the last N days")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Recent workspaces retrieved successfully")
    })
    public ResponseEntity<List<Workspace>> getRecentWorkspaces(
            @Parameter(description = "Number of days to look back") @RequestParam(defaultValue = "30") int days) {
        
        logger.debug("Getting recent workspaces (last {} days)", days);
        
        if (days <= 0) {
            days = 30; // Default to 30 days
        }
        
        try {
            List<Workspace> workspaces = workspaceService.getRecentWorkspaces(days);
            logger.info("Retrieved {} recent workspaces (last {} days)", workspaces.size(), days);
            return ResponseEntity.ok(workspaces);
        } catch (Exception e) {
            logger.error("Error retrieving recent workspaces", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Search workspaces by name
     */
    @GetMapping("/search")
    @Operation(summary = "Search workspaces by name", description = "Searches for workspaces by name (case insensitive)")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Search completed successfully"),
        @ApiResponse(responseCode = "400", description = "Invalid search query")
    })
    public ResponseEntity<List<Workspace>> searchWorkspaces(
            @Parameter(description = "Name to search for") @RequestParam String name) {
        
        logger.debug("Searching workspaces by name: {}", name);
        
        if (name == null || name.trim().isEmpty()) {
            return ResponseEntity.badRequest().build();
        }
        
        try {
            List<Workspace> workspaces = workspaceService.searchWorkspacesByName(name);
            logger.info("Found {} workspaces matching name: {}", workspaces.size(), name);
            return ResponseEntity.ok(workspaces);
        } catch (Exception e) {
            logger.error("Error searching workspaces by name: {}", name, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Count workspaces by tenant
     */
    @GetMapping("/tenant/{tenantId}/count")
    @Operation(summary = "Count workspaces by tenant", description = "Returns the number of workspaces for a tenant")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Count retrieved successfully"),
        @ApiResponse(responseCode = "400", description = "Invalid tenant ID")
    })
    public ResponseEntity<Long> countWorkspacesByTenant(
            @Parameter(description = "Tenant ID") @PathVariable String tenantId) {
        
        logger.debug("Counting workspaces for tenant: {}", tenantId);
        
        if (tenantId == null || tenantId.trim().isEmpty()) {
            return ResponseEntity.badRequest().build();
        }
        
        try {
            long count = workspaceService.countWorkspacesByTenant(tenantId);
            logger.info("Tenant {} has {} workspaces", tenantId, count);
            return ResponseEntity.ok(count);
        } catch (Exception e) {
            logger.error("Error counting workspaces for tenant: {}", tenantId, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Delete workspace
     */
    @DeleteMapping("/{workspaceId}")
    @Operation(summary = "Delete workspace", description = "Deletes a workspace by its ID")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "204", description = "Workspace deleted successfully"),
        @ApiResponse(responseCode = "404", description = "Workspace not found")
    })
    public ResponseEntity<Void> deleteWorkspace(
            @Parameter(description = "Workspace ID") @PathVariable String workspaceId) {
        
        logger.info("Deleting workspace: {}", workspaceId);
        
        try {
            boolean deleted = workspaceService.deleteWorkspace(workspaceId);
            
            if (deleted) {
                logger.info("Successfully deleted workspace: {}", workspaceId);
                return ResponseEntity.noContent().build();
            } else {
                logger.warn("Workspace not found for deletion: {}", workspaceId);
                return ResponseEntity.notFound().build();
            }
        } catch (Exception e) {
            logger.error("Error deleting workspace: {}", workspaceId, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Get workspaces by tenant schema
     */
    @GetMapping("/schema/{tenantSchema}")
    @Operation(summary = "Get workspaces by tenant schema", description = "Retrieves all workspaces for a specific tenant schema")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Workspaces retrieved successfully"),
        @ApiResponse(responseCode = "400", description = "Invalid tenant schema")
    })
    public ResponseEntity<List<Workspace>> getWorkspacesByTenantSchema(
            @Parameter(description = "Tenant Schema") @PathVariable String tenantSchema) {
        
        logger.debug("Getting workspaces for tenant schema: {}", tenantSchema);
        
        if (tenantSchema == null || tenantSchema.trim().isEmpty()) {
            return ResponseEntity.badRequest().build();
        }
        
        try {
            List<Workspace> workspaces = workspaceService.getWorkspacesByTenantSchema(tenantSchema);
            logger.info("Retrieved {} workspaces for tenant schema: {}", workspaces.size(), tenantSchema);
            return ResponseEntity.ok(workspaces);
        } catch (Exception e) {
            logger.error("Error retrieving workspaces for tenant schema: {}", tenantSchema, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * Get workspaces by tenant and tenant schema
     */
    @GetMapping("/tenant/{tenantId}/schema/{tenantSchema}")
    @Operation(summary = "Get workspaces by tenant and schema", description = "Retrieves all workspaces for a specific tenant and schema combination")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Workspaces retrieved successfully"),
        @ApiResponse(responseCode = "400", description = "Invalid parameters")
    })
    public ResponseEntity<List<Workspace>> getWorkspacesByTenantAndSchema(
            @Parameter(description = "Tenant ID") @PathVariable String tenantId,
            @Parameter(description = "Tenant Schema") @PathVariable String tenantSchema) {
        
        logger.debug("Getting workspaces for tenant: {} and schema: {}", tenantId, tenantSchema);
        
        if (tenantId == null || tenantId.trim().isEmpty() || 
            tenantSchema == null || tenantSchema.trim().isEmpty()) {
            return ResponseEntity.badRequest().build();
        }
        
        try {
            List<Workspace> workspaces = workspaceService.getWorkspacesByTenantAndSchema(tenantId, tenantSchema);
            logger.info("Retrieved {} workspaces for tenant: {} and schema: {}", workspaces.size(), tenantId, tenantSchema);
            return ResponseEntity.ok(workspaces);
        } catch (Exception e) {
            logger.error("Error retrieving workspaces for tenant: {} and schema: {}", tenantId, tenantSchema, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}
