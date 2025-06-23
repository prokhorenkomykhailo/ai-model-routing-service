package com.lucid.automation.airouting.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * DTO for User API responses with simplified user information
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserResponseDTO {
    
    private String id;
    private String tenantId;
    private String workspaceId;
    private String slackUserId;
    private String name;
    private String displayName;
    private String email;
    private String title;
    private String firstName;
    private String lastName;
    private String avatarHash;
    private String image24;
    private String image48;
    private String teamName;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    private LocalDateTime lastSeenAt;
    private Long messageCount;
    private Boolean isActive;
    
    /**
     * Convert a User entity to UserResponseDTO
     */
    public static UserResponseDTO fromUser(com.lucid.automation.airouting.model.User user) {
        if (user == null) {
            return null;
        }
        
        return UserResponseDTO.builder()
                .id(user.getId())
                .tenantId(user.getTenantId())
                .workspaceId(user.getWorkspaceId())
                .slackUserId(user.getSlackUserId())
                .name(user.getName())
                .displayName(user.getDisplayName())
                .email(user.getEmail())
                .title(user.getTitle())
                .firstName(user.getFirstName())
                .lastName(user.getLastName())
                .avatarHash(user.getAvatarHash())
                .image24(user.getImage24())
                .image48(user.getImage48())
                .teamName(user.getTeamName())
                .createdAt(user.getCreatedAt())
                .updatedAt(user.getUpdatedAt())
                .lastSeenAt(user.getLastSeenAt())
                .messageCount(user.getMessageCount())
                .isActive(user.getIsActive())
                .build();
    }
}
