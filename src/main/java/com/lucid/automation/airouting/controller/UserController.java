package com.lucid.automation.airouting.controller;

import com.lucid.automation.airouting.audit.Audit;
import com.lucid.automation.airouting.service.UserService;
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
     * Delete user by ID
     */
    @DeleteMapping("/id/{userId}")
    @Audit(action = "AI_USER_DELETE", description = "User deleted user by ID")
    @Operation(summary = "Delete user by ID", description = "Deletes a specific user by their ID")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "204", description = "User deleted successfully"),
        @ApiResponse(responseCode = "404", description = "User not found"),
        @ApiResponse(responseCode = "400", description = "Invalid user ID"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Void> deleteUserById(
            @Parameter(description = "User ID (format: tenantId:workspaceId:uniqueUserId) - legacy slackUserId format is accepted during migration", required = true)
            @PathVariable String userId) {
        try {
            if (userId == null || userId.trim().isEmpty()) {
                logger.warn("Invalid user ID provided: {}", userId);
                return ResponseEntity.badRequest().build();
            }

            logger.info("Deleting user by ID: {}", userId);

            // Check if user exists before attempting delete
            if (!userService.getUserById(userId).isPresent()) {
                logger.warn("User not found: {}", userId);
                return ResponseEntity.notFound().build();
            }

            // Delete the user
            userService.deleteUser(userId);
            logger.info("Successfully deleted user: {}", userId);
            return ResponseEntity.noContent().build();

        } catch (Exception e) {
            logger.error("Error deleting user by ID {}: {}", userId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}
