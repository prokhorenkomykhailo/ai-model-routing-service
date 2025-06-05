package com.lucid.automation.airouting.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * DTO for Tenant information (matches the auth-service TenantDTO)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class TenantDTO {
    private Long id;
    private UUID tenantId;
    private String name;
    private String email;
    private String description;
    private String domainUrl;
    private String schemaName;
    private String status;  // Changed from TenantStatus enum to String to match JSON
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
     * Tenant settings class - matches auth-service TenantSettings
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TenantSettings {
        private String[] allowedDomains;
        private String[] features;
        private BrandingSettings branding;
        private AuthSettings auth;
        private SubscriptionSettings subscription;
        private NotificationSettings notifications;
        private DataRetentionSettings dataRetention;
        private LocaleSettings locale;
        private ApiSettings api;
        
        @Data
        @Builder
        @NoArgsConstructor
        @AllArgsConstructor
        @JsonIgnoreProperties(ignoreUnknown = true)
        public static class BrandingSettings {
            private String logoUrl;
            private String primaryColor;
        }
        
        @Data
        @Builder
        @NoArgsConstructor
        @AllArgsConstructor
        @JsonIgnoreProperties(ignoreUnknown = true)
        public static class AuthSettings {
            private boolean ssoEnabled;
            private String samlProvider;
            private boolean mfaRequired;
        }
        
        @Data
        @Builder
        @NoArgsConstructor
        @AllArgsConstructor
        @JsonIgnoreProperties(ignoreUnknown = true)
        public static class SubscriptionSettings {
            private String plan;
            private int userLimit;
            private String renewalDate;
        }
        
        @Data
        @Builder
        @NoArgsConstructor
        @AllArgsConstructor
        @JsonIgnoreProperties(ignoreUnknown = true)
        public static class NotificationSettings {
            private boolean emailEnabled;
            private String webhookUrl;
        }
        
        @Data
        @Builder
        @NoArgsConstructor
        @AllArgsConstructor
        @JsonIgnoreProperties(ignoreUnknown = true)
        public static class DataRetentionSettings {
            private int retentionDays;
        }
        
        @Data
        @Builder
        @NoArgsConstructor
        @AllArgsConstructor
        @JsonIgnoreProperties(ignoreUnknown = true)
        public static class LocaleSettings {
            private String language;
            private String timezone;
        }
        
        @Data
        @Builder
        @NoArgsConstructor
        @AllArgsConstructor
        @JsonIgnoreProperties(ignoreUnknown = true)
        public static class ApiSettings {
            private int rateLimit;
            private String[] allowedIps;
        }
    }
}
