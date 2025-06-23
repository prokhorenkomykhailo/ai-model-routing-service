package com.lucid.automation.airouting.controller;

import com.lucid.automation.airouting.dto.UserResponseDTO;
import com.lucid.automation.airouting.model.User;
import com.lucid.automation.airouting.service.UserService;
import com.lucid.automation.airouting.service.UserService.UserStats;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * REST Controller for managing users stored in Redis
 */
@RestController
@RequestMapping("/api/users")
@Tag(name = "User Management", description = "API for managing users stored in Redis")
public class UserController {
    
    private static final Logger logger = LoggerFactory.getLogger(UserController.class);
    
    private final UserService userService;
    
    public UserController(UserService userService) {
        this.userService = userService;
    }
    
    /**
     * Get all users from Redis
     */
    @GetMapping
    @Operation(summary = "Get all users", description = "Retrieves all users stored in Redis")
    public ResponseEntity<List<User>> getAllUsers() {
        try {
            logger.info("Fetching all users from Redis");
            List<User> users = userService.getAllUsers();
            logger.info("Successfully retrieved {} users from Redis", users.size());
            return ResponseEntity.ok(users);
        } catch (Exception e) {
            logger.error("Error fetching all users from Redis: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * Get users by tenant ID
     */
    @GetMapping("/tenant/{tenantId}")
    @Operation(summary = "Get users by tenant", description = "Retrieves all users for a specific tenant")
    public ResponseEntity<List<User>> getUsersByTenant(
            @Parameter(description = "Tenant ID", required = true)
            @PathVariable String tenantId) {
        try {
            logger.info("Fetching users for tenant: {}", tenantId);
            List<User> users = userService.getUsersByTenant(tenantId);
            logger.info("Successfully retrieved {} users for tenant: {}", users.size(), tenantId);
            return ResponseEntity.ok(users);
        } catch (Exception e) {
            logger.error("Error fetching users for tenant {}: {}", tenantId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * Get users by workspace
     */
    @GetMapping("/workspace/{tenantId}/{workspaceId}")
    @Operation(summary = "Get users by workspace", description = "Retrieves all users for a specific workspace")
    public ResponseEntity<List<User>> getUsersByWorkspace(
            @Parameter(description = "Tenant ID", required = true)
            @PathVariable String tenantId,
            @Parameter(description = "Workspace ID", required = true)
            @PathVariable String workspaceId) {
        try {
            logger.info("Fetching users for workspace: tenantId={}, workspaceId={}", tenantId, workspaceId);
            List<User> users = userService.getUsersForWorkspace(tenantId, workspaceId);
            logger.info("Successfully retrieved {} users for workspace: {}", users.size(), workspaceId);
            return ResponseEntity.ok(users);
        } catch (Exception e) {
            logger.error("Error fetching users for workspace {}/{}: {}", tenantId, workspaceId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * Get user by ID
     */
    @GetMapping("/id/{userId}")
    @Operation(summary = "Get user by ID", description = "Retrieves a specific user by their ID")
    public ResponseEntity<User> getUserById(
            @Parameter(description = "User ID (format: tenantId:workspaceId:slackUserId)", required = true)
            @PathVariable String userId) {
        try {
            logger.info("Fetching user by ID: {}", userId);
            Optional<User> user = userService.getUserById(userId);
            
            if (user.isPresent()) {
                logger.info("Successfully retrieved user: {}", userId);
                return ResponseEntity.ok(user.get());
            } else {
                logger.warn("User not found: {}", userId);
                return ResponseEntity.notFound().build();
            }
        } catch (Exception e) {
            logger.error("Error fetching user by ID {}: {}", userId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * Get user by Slack user ID across all tenants/workspaces
     */
    @GetMapping("/slack/{slackUserId}")
    @Operation(summary = "Get users by Slack ID", description = "Retrieves all users with a specific Slack user ID")
    public ResponseEntity<List<User>> getUsersBySlackId(
            @Parameter(description = "Slack User ID", required = true)
            @PathVariable String slackUserId) {
        try {
            logger.info("Fetching users by Slack ID: {}", slackUserId);
            List<User> users = userService.getUsersBySlackId(slackUserId);
            logger.info("Successfully retrieved {} users with Slack ID: {}", users.size(), slackUserId);
            return ResponseEntity.ok(users);
        } catch (Exception e) {
            logger.error("Error fetching users by Slack ID {}: {}", slackUserId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * Get user statistics for a workspace
     */
    @GetMapping("/stats/{tenantId}/{workspaceId}")
    @Operation(summary = "Get user statistics", description = "Retrieves user statistics for a specific workspace")
    public ResponseEntity<UserStats> getUserStats(
            @Parameter(description = "Tenant ID", required = true)
            @PathVariable String tenantId,
            @Parameter(description = "Workspace ID", required = true)
            @PathVariable String workspaceId) {
        try {
            logger.info("Fetching user stats for workspace: tenantId={}, workspaceId={}", tenantId, workspaceId);
            UserStats stats = userService.getUserStats(tenantId, workspaceId);
            logger.info("Successfully retrieved user stats for workspace: {}", workspaceId);
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            logger.error("Error fetching user stats for workspace {}/{}: {}", tenantId, workspaceId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * Search users by name or email
     */
    @GetMapping("/search")
    @Operation(summary = "Search users", description = "Search users by name or email")
    public ResponseEntity<List<User>> searchUsers(
            @Parameter(description = "Search query (name or email)")
            @RequestParam String query,
            @Parameter(description = "Tenant ID (optional)")
            @RequestParam(required = false) String tenantId,
            @Parameter(description = "Workspace ID (optional)")
            @RequestParam(required = false) String workspaceId) {
        try {
            logger.info("Searching users with query: '{}', tenantId: {}, workspaceId: {}", query, tenantId, workspaceId);
            List<User> users = userService.searchUsers(query, tenantId, workspaceId);
            logger.info("Successfully found {} users matching search query", users.size());
            return ResponseEntity.ok(users);
        } catch (Exception e) {
            logger.error("Error searching users with query '{}': {}", query, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * Get active users for a workspace
     */
    @GetMapping("/active/{tenantId}/{workspaceId}")
    @Operation(summary = "Get active users", description = "Retrieves only active users for a specific workspace")
    public ResponseEntity<List<User>> getActiveUsers(
            @Parameter(description = "Tenant ID", required = true)
            @PathVariable String tenantId,
            @Parameter(description = "Workspace ID", required = true)
            @PathVariable String workspaceId) {
        try {
            logger.info("Fetching active users for workspace: tenantId={}, workspaceId={}", tenantId, workspaceId);
            List<User> users = userService.getActiveUsers(tenantId, workspaceId);
            logger.info("Successfully retrieved {} active users for workspace: {}", users.size(), workspaceId);
            return ResponseEntity.ok(users);
        } catch (Exception e) {
            logger.error("Error fetching active users for workspace {}/{}: {}", tenantId, workspaceId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * Health check endpoint for Redis connectivity
     */
    @GetMapping("/health")
    @Operation(summary = "Health check", description = "Check Redis connectivity and return basic stats")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        try {
            logger.info("Performing Redis health check");
            Map<String, Object> health = userService.getRedisHealthInfo();
            logger.info("Redis health check completed successfully");
            return ResponseEntity.ok(health);
        } catch (Exception e) {
            logger.error("Redis health check failed: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of(
                            "status", "DOWN",
                            "error", e.getMessage(),
                            "timestamp", System.currentTimeMillis()
                    ));
        }
    }
    
    /**
     * Get all users from Redis (DTO format)
     */
    @GetMapping("/dto")
    @Operation(summary = "Get all users (DTO)", description = "Retrieves all users stored in Redis in simplified DTO format")
    public ResponseEntity<List<UserResponseDTO>> getAllUsersDTO() {
        try {
            logger.info("Fetching all users from Redis (DTO format)");
            List<User> users = userService.getAllUsers();
            List<UserResponseDTO> userDTOs = users.stream()
                    .map(UserResponseDTO::fromUser)
                    .collect(java.util.stream.Collectors.toList());
            logger.info("Successfully retrieved {} users from Redis (DTO format)", userDTOs.size());
            return ResponseEntity.ok(userDTOs);
        } catch (Exception e) {
            logger.error("Error fetching all users from Redis (DTO format): {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * Get users by workspace (DTO format)
     */
    @GetMapping("/dto/workspace/{tenantId}/{workspaceId}")
    @Operation(summary = "Get users by workspace (DTO)", description = "Retrieves all users for a specific workspace in DTO format")
    public ResponseEntity<List<UserResponseDTO>> getUsersByWorkspaceDTO(
            @Parameter(description = "Tenant ID", required = true)
            @PathVariable String tenantId,
            @Parameter(description = "Workspace ID", required = true)
            @PathVariable String workspaceId) {
        try {
            logger.info("Fetching users for workspace (DTO): tenantId={}, workspaceId={}", tenantId, workspaceId);
            List<User> users = userService.getUsersForWorkspace(tenantId, workspaceId);
            List<UserResponseDTO> userDTOs = users.stream()
                    .map(UserResponseDTO::fromUser)
                    .collect(java.util.stream.Collectors.toList());
            logger.info("Successfully retrieved {} users for workspace (DTO): {}", userDTOs.size(), workspaceId);
            return ResponseEntity.ok(userDTOs);
        } catch (Exception e) {
            logger.error("Error fetching users for workspace (DTO) {}/{}: {}", tenantId, workspaceId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}
