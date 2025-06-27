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
}
