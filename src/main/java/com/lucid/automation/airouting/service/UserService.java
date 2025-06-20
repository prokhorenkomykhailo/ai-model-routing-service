package com.lucid.automation.airouting.service;

import com.lucid.automation.slackingestion.dto.messaging.IngestionEventDTO;
import com.lucid.automation.slackingestion.dto.messaging.SlackUserDTO;
import com.lucid.automation.airouting.model.User;
import com.lucid.automation.airouting.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.List;
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
        
        SlackUserDTO userData = ingestionEvent.getUser();
        String tenantId = ingestionEvent.getTenantId();
        String workspaceId = ingestionEvent.getMessage() != null ? ingestionEvent.getMessage().getTeamId() : null;
        String slackUserId = userData.getSlackUserId();
        
        // Validate required fields
        if (!StringUtils.hasText(tenantId) || !StringUtils.hasText(workspaceId) || !StringUtils.hasText(slackUserId)) {
            logger.warn("Missing required user data: tenantId={}, workspaceId={}, slackUserId={}", 
                       tenantId, workspaceId, slackUserId);
            return null;
        }
        
        try {
            // Generate user ID
            String userId = User.generateId(tenantId, workspaceId, slackUserId);
            
            // Check if user already exists
            Optional<User> existingUser = userRepository.findById(userId);
            
            User user;
            if (existingUser.isPresent()) {
                user = existingUser.get();
                logger.debug("Updating existing user: {}", userId);
                
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
            
            logger.info("Successfully saved user to Redis: userId={}, name={}, email={}, messageCount={}", 
                       savedUser.getId(), savedUser.getName(), savedUser.getEmail(), savedUser.getMessageCount());
            
            return savedUser;
            
        } catch (Exception e) {
            logger.error("Error creating/updating user from ingestion event: {}", e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * Creates a new User from ingestion event data
     */
    private User createUserFromEvent(SlackUserDTO userData, IngestionEventDTO ingestionEvent) {
        String tenantId = ingestionEvent.getTenantId();
        String workspaceId = ingestionEvent.getMessage().getTeamId();
        String slackUserId = userData.getSlackUserId();
        String userId = User.generateId(tenantId, workspaceId, slackUserId);
        
        return User.builder()
                .id(userId)
                .tenantId(tenantId)
                .workspaceId(workspaceId)
                .slackUserId(slackUserId)
                .teamId(userData.getTeamId())
                .name(userData.getName())
                .emailConfirmed(userData.getEmailConfirmed())
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
                .imageOriginal(userData.getImageOriginal())
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
    private void updateUserFromEvent(User user, SlackUserDTO userData, IngestionEventDTO ingestionEvent) {
        // Update all user profile fields with latest data
        user.setTeamId(userData.getTeamId());
        user.setName(userData.getName());
        user.setEmailConfirmed(userData.getEmailConfirmed());
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
     * Get user by tenant, workspace, and slack user ID
     */
    public Optional<User> getUser(String tenantId, String workspaceId, String slackUserId) {
        return userRepository.findByTenantIdAndWorkspaceIdAndSlackUserId(tenantId, workspaceId, slackUserId);
    }
    
    /**
     * Get all users for a workspace
     */
    public List<User> getUsersForWorkspace(String tenantId, String workspaceId) {
        return userRepository.findByTenantIdAndWorkspaceId(tenantId, workspaceId);
    }
    
    /**
     * Check if user exists
     */
    public boolean userExists(String tenantId, String workspaceId, String slackUserId) {
        return userRepository.existsByTenantIdAndWorkspaceIdAndSlackUserId(tenantId, workspaceId, slackUserId);
    }
    
    /**
     * Get user statistics
     */
    public UserStats getUserStats(String tenantId, String workspaceId) {
        List<User> users = getUsersForWorkspace(tenantId, workspaceId);
        
        long totalUsers = users.size();
        long activeUsers = users.stream().filter(u -> Boolean.TRUE.equals(u.getIsActive())).count();
        long totalMessages = users.stream().mapToLong(u -> u.getMessageCount() != null ? u.getMessageCount() : 0L).sum();
        
        return new UserStats(totalUsers, activeUsers, totalMessages);
    }
    
    /**
     * Record class for user statistics
     */
    public record UserStats(long totalUsers, long activeUsers, long totalMessages) {}
}
