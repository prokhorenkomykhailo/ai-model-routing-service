package com.lucid.automation.airouting.util;

import org.springframework.util.StringUtils;

import java.util.UUID;

/**
 * Utility class for validating tenant IDs and related tenant operations
 */
public final class TenantValidationUtil {

    private TenantValidationUtil() {
        // Utility class - prevent instantiation
    }

    /**
     * UUID representing an invalid/null tenant ID pattern
     */
    public static final String INVALID_TENANT_UUID = "00000000-0000-0000-0000-000000000000";

    /**
     * Validates if a tenant ID is valid and should allow AI processing.
     *
     * A tenant ID is considered invalid if:
     * - It is null
     * - It is empty or blank
     * - It matches the invalid UUID pattern (00000000-0000-0000-0000-000000000000)
     *
     * @param tenantId The tenant ID to validate
     * @return true if the tenant ID is valid and AI requests should be processed, false otherwise
     */
    public static boolean isValidTenantId(String tenantId) {
        if (!StringUtils.hasText(tenantId)) {
            return false;
        }

        String cleanedTenantId = tenantId.trim();

        // Check if it matches the invalid UUID pattern
        if (INVALID_TENANT_UUID.equals(cleanedTenantId)) {
            return false;
        }

        // Additional validation: ensure it's a valid UUID format
        try {
            UUID.fromString(cleanedTenantId);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Checks if a tenant ID is invalid and should prevent AI processing.
     * This is the inverse of isValidTenantId() for better readability in conditional logic.
     *
     * @param tenantId The tenant ID to validate
     * @return true if the tenant ID is invalid and AI requests should be rejected, false otherwise
     */
    public static boolean isInvalidTenantId(String tenantId) {
        return !isValidTenantId(tenantId);
    }

    /**
     * Gets a descriptive message explaining why a tenant ID is invalid.
     * Useful for logging and error messages.
     *
     * @param tenantId The tenant ID to analyze
     * @return A descriptive error message
     */
    public static String getInvalidTenantIdReason(String tenantId) {
        if (tenantId == null) {
            return "Tenant ID is null";
        }

        if (tenantId.trim().isEmpty()) {
            return "Tenant ID is empty or blank";
        }

        if (INVALID_TENANT_UUID.equals(tenantId.trim())) {
            return "Tenant ID matches invalid UUID pattern (00000000-0000-0000-0000-000000000000)";
        }

        try {
            UUID.fromString(tenantId.trim());
            return "Tenant ID is valid"; // Should not reach here if called correctly
        } catch (IllegalArgumentException e) {
            return "Tenant ID is not a valid UUID format: " + e.getMessage();
        }
    }

    /**
     * Sanitizes and validates a tenant ID, returning a clean version or null if invalid.
     *
     * @param tenantId The tenant ID to sanitize
     * @return The sanitized tenant ID if valid, null if invalid
     */
    public static String sanitizeTenantId(String tenantId) {
        if (!StringUtils.hasText(tenantId)) {
            return null;
        }

        String cleanedTenantId = tenantId.trim();

        if (isValidTenantId(cleanedTenantId)) {
            return cleanedTenantId;
        }

        return null;
    }
}
