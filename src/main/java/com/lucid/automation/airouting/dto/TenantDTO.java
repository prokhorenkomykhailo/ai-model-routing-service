package com.lucid.automation.airouting.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * DTO for Tenant information (matches the auth-service TenantDTO)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TenantDTO {
    private Long id;
    private UUID tenantId;
    private String name;
    private String email;
    private String description;
    private String domainUrl;
    private String schemaName;
    private TenantStatus status;
    private TenantSettings settings;
    private LocalDateTime createdAt;
    private String workspaceName;
    private UUID workspaceId;
    private LocalDateTime updatedAt;
    private String createdBy;
    private TenantRole role;
    
    /**
     * Tenant status enum
     */
    public enum TenantStatus {
        ACTIVE, SUSPENDED, DELETED
    }
    
    /**
     * Tenant role enum
     */
    public enum TenantRole {
        ADMIN, MEMBER, GUEST
    }
    
    /**
     * Tenant settings class
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TenantSettings {
        private Boolean allowGuestAccess;
        private Boolean enableSlackIntegration;
        private Boolean enableEmailNotifications;
        // Add more settings as needed
    }
}
