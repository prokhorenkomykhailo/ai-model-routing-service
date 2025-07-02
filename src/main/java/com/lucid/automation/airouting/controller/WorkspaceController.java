package com.lucid.automation.airouting.controller;

import com.lucid.automation.airouting.dto.WorkspaceStats;
import com.lucid.automation.airouting.model.Workspace;
import com.lucid.automation.airouting.service.WorkspaceService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

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
     * Delete a workspace by ID
     */
    @DeleteMapping("/{id}")
    @Operation(summary = "Delete workspace by ID", description = "Deletes a workspace and all its messages by its ID")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "204", description = "Workspace deleted successfully"),
        @ApiResponse(responseCode = "404", description = "Workspace not found"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Void> deleteWorkspaceById(@PathVariable String id) {
        logger.info("Deleting workspace with ID: {}", id);
        try {
            boolean deleted = workspaceService.deleteWorkspaceById(id);
            if (deleted) {
                logger.info("Workspace with ID {} deleted", id);
                return ResponseEntity.noContent().build();
            } else {
                logger.warn("Workspace with ID {} not found", id);
                return ResponseEntity.notFound().build();
            }
        } catch (Exception e) {
            logger.error("Error deleting workspace with ID: {}", id, e);
            return ResponseEntity.internalServerError().build();
        }
    }
}
