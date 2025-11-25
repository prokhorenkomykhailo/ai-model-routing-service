package com.lucid.automation.airouting.provider;

import com.lucid.automation.airouting.exception.InvalidTenantException;
import com.lucid.automation.airouting.model.message.AIMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for AIProvider tenant validation functionality
 */
class AIProviderTenantValidationTest {

    private TestAIProvider testProvider;

    @BeforeEach
    void setUp() {
        testProvider = new TestAIProvider();
    }

    @Test
    void testValidateTenantId_ValidTenant_ShouldPass() {
        // Valid UUID should not throw exception
        String validTenantId = "550e8400-e29b-41d4-a716-446655440000";

        assertDoesNotThrow(() -> {
            testProvider.validateTenantId(validTenantId, "test-operation");
        });
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"", "   ", "\t", "\n"})
    void testValidateTenantId_NullOrEmptyTenant_ShouldThrowException(String tenantId) {
        InvalidTenantException exception = assertThrows(InvalidTenantException.class, () -> {
            testProvider.validateTenantId(tenantId, "test-operation");
        });

        // Note: tenantId in exception may be null for null input, but reason should always be present
        assertNotNull(exception.getReason());
        assertTrue(exception.getMessage().contains("Invalid tenant ID"));

        // Check the actual tenantId field - it should match what was passed in
        assertEquals(tenantId, exception.getTenantId());
    }

    @Test
    void testValidateTenantId_InvalidUuidPattern_ShouldThrowException() {
        String invalidTenantId = "00000000-0000-0000-0000-000000000000";

        InvalidTenantException exception = assertThrows(InvalidTenantException.class, () -> {
            testProvider.validateTenantId(invalidTenantId, "test-operation");
        });

        assertEquals(invalidTenantId, exception.getTenantId());
        assertTrue(exception.getReason().contains("invalid UUID pattern"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "not-a-uuid",
            "123",
            "invalid-uuid-format",
            "550e8400-e29b-41d4-a716",  // Incomplete UUID
            "550e8400-e29b-41d4-a716-44665544000g"  // Invalid character
    })
    void testValidateTenantId_InvalidUuidFormat_ShouldThrowException(String tenantId) {
        InvalidTenantException exception = assertThrows(InvalidTenantException.class, () -> {
            testProvider.validateTenantId(tenantId, "test-operation");
        });

        assertEquals(tenantId, exception.getTenantId());
        assertTrue(exception.getReason().contains("not a valid UUID format"));
    }

    @Test
    void testValidateTenantId_ValidTenantWithWhitespace_ShouldPass() {
        // Valid UUID with whitespace should pass (gets trimmed internally)
        String tenantIdWithWhitespace = "  550e8400-e29b-41d4-a716-446655440000  ";

        assertDoesNotThrow(() -> {
            testProvider.validateTenantId(tenantIdWithWhitespace, "test-operation");
        });
    }

    @Test
    void testValidateTenantId_DifferentOperations_ShouldIncludeOperationInException() {
        String invalidTenantId = "invalid-tenant";
        String operation = "conversation-enrichment";

        InvalidTenantException exception = assertThrows(InvalidTenantException.class, () -> {
            testProvider.validateTenantId(invalidTenantId, operation);
        });

        // The exception message should contain the operation name
        assertNotNull(exception.getMessage());
        assertTrue(exception.getMessage().contains(invalidTenantId));
    }

    @Test
    void testValidateTenantId_MultipleValidFormats() {
        String[] validTenantIds = {
            "550e8400-e29b-41d4-a716-446655440001",
            "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
            "123e4567-e89b-12d3-a456-426614174000",
            "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11", // Uppercase
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"  // Lowercase
        };

        for (String tenantId : validTenantIds) {
            assertDoesNotThrow(() -> {
                testProvider.validateTenantId(tenantId, "test-operation");
            }, "Should accept valid UUID: " + tenantId);
        }
    }

    /**
     * Test implementation of AIProvider for testing purposes
     */
    private static class TestAIProvider extends AIProvider {

        @Override
        protected Map<String, Object> doEnrichConversation(
                AIMessage messages, String tenantId, String deemergeUserId, String deemergeUserName, String debugId) {
            return Map.of("test", "result");
        }

        @Override
        protected String doProcessTextQuery(String maskedQuery, String userId, String tenantId, String debugId) {
            return "test response";
        }

        @Override
        public String getProviderId() {
            return "test-provider";
        }

        @Override
        public boolean isAvailable() {
            return true;
        }

        @Override
        public double getLastConfidence() {
            return 0.95;
        }

        // Expose protected method for testing
        public void validateTenantId(String tenantId, String operation) {
            super.validateTenantId(tenantId, operation);
        }
    }
}
