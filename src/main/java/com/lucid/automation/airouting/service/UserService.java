package com.lucid.automation.airouting.service;

import com.lucid.automation.common.dto.messaging.IngestionEventDTO;
import com.lucid.automation.common.dto.messaging.IngestionUserDTO;
import com.lucid.automation.common.dto.messaging.IngestionMessageDTO;
import com.lucid.automation.airouting.model.User;
import com.lucid.automation.airouting.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.Optional;

/**
 * Service for managing users in Redis
 */
@Service
public class UserService {

    private static final Logger logger = LoggerFactory.getLogger(UserService.class);

    private final UserRepository userRepository;

    public UserService(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    /**
     * Creates or updates a user from ingestion event data
     *
     * @param ingestionEvent The ingestion event containing user data
     * @return The saved user, or null if the user data is invalid
     */
    public User createOrUpdateUser(IngestionEventDTO ingestionEvent) {
        if (ingestionEvent == null || ingestionEvent.getUser() == null) {
            logger.debug("No user data in ingestion event, skipping user creation/update");
            return null;
        }

        IngestionUserDTO userData = ingestionEvent.getUser();
        String tenantId = ingestionEvent.getTenantId();
        // Get workspace ID from message using unified DTO helper method
        IngestionMessageDTO message = ingestionEvent.getMessage();
        String workspaceId = message != null ? message.getBestWorkspaceId() : null;
        // Resolve user key: prefer uniqueUserId over slackUserId
        String userKey = userData.getUniqueUserId() != null ? userData.getUniqueUserId() : userData.getSlackUserId();

        // Validate required fields
        if (!StringUtils.hasText(tenantId) || !StringUtils.hasText(workspaceId) || !StringUtils.hasText(userKey)) {
            logger.warn("Missing required user data: tenantId={}, workspaceId={}, userKey={}",
                       tenantId, workspaceId, userKey);
            return null;
        }

        try {
            // Generate user ID using resolved user key (uniqueUserId or slackUserId)
            String userId = User.generateId(tenantId, workspaceId, userKey);

            // Check if user already exists
            Optional<User> existingUser = userRepository.findById(userId);

            User user;
            if (existingUser.isPresent()) {
                user = existingUser.get();
                // Update user data from the new event
                updateUserFromEvent(user, userData, ingestionEvent);
                user.updateActivity(); // Update timestamps and message count

            } else {
                logger.info("Creating new user: {}", userId);

                // Create new user
                user = createUserFromEvent(userData, ingestionEvent);
                user.setCreatedIfNew();
                user.updateActivity();
            }

            // Save user to Redis
            User savedUser = userRepository.save(user);
            return savedUser;

        } catch (Exception e) {
            logger.error("Error creating/updating user from ingestion event: {}", e.getMessage(), e);
            return null;
        }
    }

    /**
     * Creates a new User from ingestion event data
     */
    private User createUserFromEvent(IngestionUserDTO userData, IngestionEventDTO ingestionEvent) {
        String tenantId = ingestionEvent.getTenantId();
        // Get workspace ID from message using unified DTO helper method
        IngestionMessageDTO message = ingestionEvent.getMessage();
        String workspaceId = message != null ? message.getBestWorkspaceId() : null;
        String slackUserId = userData.getSlackUserId();
        // Prefer uniqueUserId over slackUserId
        String uniqueUserId = userData.getUniqueUserId() != null ? userData.getUniqueUserId() : slackUserId;
        String userId = User.generateId(tenantId, workspaceId, uniqueUserId);

        return User.builder()
                .id(userId)
                .tenantId(tenantId)
                .workspaceId(workspaceId)
                .slackUserId(slackUserId)
                .uniqueUserId(uniqueUserId)
                .teamId(userData.getTeamId())
                .name(userData.getName())
                .emailConfirmed(userData.getEmailVerified())
                .displayName(userData.getDisplayName())
                .displayNameNormalized(userData.getDisplayNameNormalized())
                .realNameNormalized(userData.getRealNameNormalized())
                .email(userData.getEmail())
                .title(userData.getTitle())
                .phone(userData.getPhone())
                .firstName(userData.getFirstName())
                .lastName(userData.getLastName())
                .pronouns(userData.getPronouns())
                .statusText(userData.getStatusText())
                .avatarHash(userData.getAvatarHash())
                // Prefer avatarUrl over imageOriginal for image handling
                .imageOriginal(userData.getAvatarUrl() != null ? userData.getAvatarUrl() : userData.getImageOriginal())
                .image24(userData.getImage24())
                .image32(userData.getImage32())
                .image48(userData.getImage48())
                .image72(userData.getImage72())
                .image192(userData.getImage192())
                .image512(userData.getImage512())
                .image1024(userData.getImage1024())
                .teamName(userData.getTeamName())
                .slackUpdatedAt(userData.getSlackUpdatedAt())
                .messageCount(0L)
                .isActive(true)
                .build();
    }

    /**
     * Updates an existing User with new data from ingestion event
     */
    private void updateUserFromEvent(User user, IngestionUserDTO userData, IngestionEventDTO ingestionEvent) {
        // Update all user profile fields with latest data
        user.setTeamId(userData.getTeamId());
        user.setName(userData.getName());
        user.setEmailConfirmed(userData.getEmailVerified());
        // Update uniqueUserId if provided (prefer over slackUserId)
        if (userData.getUniqueUserId() != null) {
            user.setUniqueUserId(userData.getUniqueUserId());
        }
        user.setDisplayName(userData.getDisplayName());
        user.setDisplayNameNormalized(userData.getDisplayNameNormalized());
        user.setRealNameNormalized(userData.getRealNameNormalized());
        user.setEmail(userData.getEmail());
        user.setTitle(userData.getTitle());
        user.setPhone(userData.getPhone());
        user.setFirstName(userData.getFirstName());
        user.setLastName(userData.getLastName());
        user.setPronouns(userData.getPronouns());
        user.setStatusText(userData.getStatusText());
        user.setAvatarHash(userData.getAvatarHash());
        user.setImageOriginal(userData.getImageOriginal());
        user.setImage24(userData.getImage24());
        user.setImage32(userData.getImage32());
        user.setImage48(userData.getImage48());
        user.setImage72(userData.getImage72());
        user.setImage192(userData.getImage192());
        user.setImage512(userData.getImage512());
        user.setImage1024(userData.getImage1024());
        user.setTeamName(userData.getTeamName());
        user.setSlackUpdatedAt(userData.getSlackUpdatedAt());

        // Keep user active
        user.setIsActive(true);
    }

    /**
     * Get user by ID
     */
    public Optional<User> getUserById(String userId) {
        return userRepository.findById(userId);
    }

    /**
     * Get user by tenant, workspace, and unique user ID (preferred method)
     * @param tenantId The tenant ID
     * @param workspaceId The workspace ID
     * @param uniqueUserId The unique user ID
     * @return Optional containing the user if found
     */
    public Optional<User> getUserByUniqueUserId(String tenantId, String workspaceId, String uniqueUserId) {
        // Add null checks to prevent Redis query issues
        if (tenantId == null || workspaceId == null || uniqueUserId == null) {
            logger.warn("Cannot query user with null parameters: tenantId={}, workspaceId={}, uniqueUserId={}",
                       tenantId, workspaceId, uniqueUserId);
            return Optional.empty();
        }

        try {
            return userRepository.findByTenantIdAndWorkspaceIdAndUniqueUserId(tenantId, workspaceId, uniqueUserId);
        } catch (Exception e) {
            logger.error("Error querying user with tenantId={}, workspaceId={}, uniqueUserId={}: {}",
                        tenantId, workspaceId, uniqueUserId, e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Get user by tenant, workspace, and slack user ID (legacy support)
     * @deprecated Use getUserByUniqueUserId instead
     */
    @Deprecated
    public Optional<User> getUser(String tenantId, String workspaceId, String slackUserId) {
        // Add null checks to prevent Redis query issues
        if (tenantId == null || workspaceId == null || slackUserId == null) {
            logger.warn("Cannot query user with null parameters: tenantId={}, workspaceId={}, slackUserId={}",
                       tenantId, workspaceId, slackUserId);
            return Optional.empty();
        }

        try {
            return userRepository.findByTenantIdAndWorkspaceIdAndSlackUserId(tenantId, workspaceId, slackUserId);
        } catch (Exception e) {
            logger.error("Error querying user with tenantId={}, workspaceId={}, slackUserId={}: {}",
                        tenantId, workspaceId, slackUserId, e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Delete a user by ID
     *
     * @param userId The user ID (format: tenantId:workspaceId:uniqueUserId)
     */
    public void deleteUser(String userId) {
        logger.info("Deleting user with ID: {}", userId);

        if (!StringUtils.hasText(userId)) {
            throw new IllegalArgumentException("User ID cannot be null or empty");
        }

        try {
            userRepository.deleteById(userId);
            logger.info("Successfully deleted user: {}", userId);
        } catch (Exception e) {
            logger.error("Failed to delete user {}: {}", userId, e.getMessage(), e);
            throw new RuntimeException("Failed to delete user: " + e.getMessage(), e);
        }
    }
}
