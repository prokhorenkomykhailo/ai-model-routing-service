package com.lucid.automation.airouting.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for TenantValidationUtil
 */
class TenantValidationUtilTest {

    @Test
    void testValidTenantId() {
        // Valid UUID format
        String validTenantId = "550e8400-e29b-41d4-a716-446655440000";
        assertTrue(TenantValidationUtil.isValidTenantId(validTenantId));
        assertFalse(TenantValidationUtil.isInvalidTenantId(validTenantId));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"", "   ", "\t", "\n"})
    void testInvalidTenantId_NullOrEmpty(String tenantId) {
        assertFalse(TenantValidationUtil.isValidTenantId(tenantId));
        assertTrue(TenantValidationUtil.isInvalidTenantId(tenantId));
    }

    @Test
    void testInvalidTenantId_InvalidUuidPattern() {
        String invalidTenantId = "00000000-0000-0000-0000-000000000000";
        assertFalse(TenantValidationUtil.isValidTenantId(invalidTenantId));
        assertTrue(TenantValidationUtil.isInvalidTenantId(invalidTenantId));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "not-a-uuid",
            "123",
            "invalid-uuid-format",
            "550e8400-e29b-41d4-a716",  // Incomplete UUID
            "550e8400-e29b-41d4-a716-44665544000g"  // Invalid character
    })
    void testInvalidTenantId_InvalidUuidFormat(String tenantId) {
        assertFalse(TenantValidationUtil.isValidTenantId(tenantId));
        assertTrue(TenantValidationUtil.isInvalidTenantId(tenantId));
    }

    @Test
    void testValidTenantIdWithWhitespace() {
        // Valid UUID with leading/trailing whitespace should be valid after trimming
        String tenantIdWithWhitespace = "  550e8400-e29b-41d4-a716-446655440000  ";
        assertTrue(TenantValidationUtil.isValidTenantId(tenantIdWithWhitespace));
    }

    @Test
    void testGetInvalidTenantIdReason_Null() {
        String reason = TenantValidationUtil.getInvalidTenantIdReason(null);
        assertEquals("Tenant ID is null", reason);
    }

    @Test
    void testGetInvalidTenantIdReason_Empty() {
        String reason = TenantValidationUtil.getInvalidTenantIdReason("");
        assertEquals("Tenant ID is empty or blank", reason);
    }

    @Test
    void testGetInvalidTenantIdReason_Blank() {
        String reason = TenantValidationUtil.getInvalidTenantIdReason("   ");
        assertEquals("Tenant ID is empty or blank", reason);
    }

    @Test
    void testGetInvalidTenantIdReason_InvalidPattern() {
        String reason = TenantValidationUtil.getInvalidTenantIdReason("00000000-0000-0000-0000-000000000000");
        assertEquals("Tenant ID matches invalid UUID pattern (00000000-0000-0000-0000-000000000000)", reason);
    }

    @Test
    void testGetInvalidTenantIdReason_InvalidFormat() {
        String reason = TenantValidationUtil.getInvalidTenantIdReason("not-a-uuid");
        assertTrue(reason.startsWith("Tenant ID is not a valid UUID format"));
    }

    @Test
    void testSanitizeTenantId_Valid() {
        String validTenantId = "550e8400-e29b-41d4-a716-446655440000";
        String sanitized = TenantValidationUtil.sanitizeTenantId(validTenantId);
        assertEquals(validTenantId, sanitized);
    }

    @Test
    void testSanitizeTenantId_ValidWithWhitespace() {
        String tenantIdWithWhitespace = "  550e8400-e29b-41d4-a716-446655440000  ";
        String expected = "550e8400-e29b-41d4-a716-446655440000";
        String sanitized = TenantValidationUtil.sanitizeTenantId(tenantIdWithWhitespace);
        assertEquals(expected, sanitized);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"", "   ", "not-a-uuid", "00000000-0000-0000-0000-000000000000"})
    void testSanitizeTenantId_Invalid(String tenantId) {
        String sanitized = TenantValidationUtil.sanitizeTenantId(tenantId);
        assertNull(sanitized);
    }

    @Test
    void testInvalidTenantUuidConstant() {
        assertEquals("00000000-0000-0000-0000-000000000000", TenantValidationUtil.INVALID_TENANT_UUID);
    }

    @Test
    void testMultipleDifferentValidUuids() {
        String[] validUuids = {
            "550e8400-e29b-41d4-a716-446655440001",
            "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
            "123e4567-e89b-12d3-a456-426614174000",
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
        };

        for (String uuid : validUuids) {
            assertTrue(TenantValidationUtil.isValidTenantId(uuid),
                    "UUID should be valid: " + uuid);
        }
    }

    @Test
    void testUppercaseLowercaseUuids() {
        String lowercaseUuid = "550e8400-e29b-41d4-a716-446655440000";
        String uppercaseUuid = "550E8400-E29B-41D4-A716-446655440000";
        String mixedCaseUuid = "550e8400-E29B-41d4-A716-446655440000";

        assertTrue(TenantValidationUtil.isValidTenantId(lowercaseUuid));
        assertTrue(TenantValidationUtil.isValidTenantId(uppercaseUuid));
        assertTrue(TenantValidationUtil.isValidTenantId(mixedCaseUuid));
    }
}
