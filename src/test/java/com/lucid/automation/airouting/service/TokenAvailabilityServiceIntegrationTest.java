package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.client.AuthTokenAvailableClient;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Integration test for TokenAvailabilityService
 * Tests the integration with auth service token availability endpoint
 */
@ExtendWith(MockitoExtension.class)
public class TokenAvailabilityServiceIntegrationTest {

    @Mock
    private AuthTokenAvailableClient authTokenAvailableClient;

    private TokenAvailabilityService tokenAvailabilityService;
    private MeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        tokenAvailabilityService = new TokenAvailabilityService(authTokenAvailableClient, meterRegistry);
    }

    @Test
    void testTokenAvailable_AuthServiceReturnsTrue() {
        // Given
        String tenantId = "123e4567-e89b-12d3-a456-426614174000";
        when(authTokenAvailableClient.getTenantTokenAvailable(tenantId, "application/json"))
                .thenReturn(ResponseEntity.ok(true));

        // When
        boolean result = tokenAvailabilityService.isTokenAvailable(tenantId);

        // Then
        assertTrue(result);
        verify(authTokenAvailableClient).getTenantTokenAvailable(tenantId, "application/json");

        // Verify metrics
        assertEquals(1.0, meterRegistry.counter("ai.tokens.available.count").count());
        assertEquals(0.0, meterRegistry.counter("ai.tokens.unavailable.count").count());
    }

    @Test
    void testTokenAvailable_AuthServiceReturnsFalse() {
        // Given
        String tenantId = "123e4567-e89b-12d3-a456-426614174000";
        when(authTokenAvailableClient.getTenantTokenAvailable(tenantId, "application/json"))
                .thenReturn(ResponseEntity.ok(false));

        // When
        boolean result = tokenAvailabilityService.isTokenAvailable(tenantId);

        // Then
        assertFalse(result);
        verify(authTokenAvailableClient).getTenantTokenAvailable(tenantId, "application/json");

        // Verify metrics
        assertEquals(0.0, meterRegistry.counter("ai.tokens.available.count").count());
        assertEquals(1.0, meterRegistry.counter("ai.tokens.unavailable.count").count());
    }

    @Test
    void testTokenAvailable_AuthServiceError() {
        // Given
        String tenantId = "123e4567-e89b-12d3-a456-426614174000";
        when(authTokenAvailableClient.getTenantTokenAvailable(tenantId, "application/json"))
                .thenReturn(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null));

        // When
        boolean result = tokenAvailabilityService.isTokenAvailable(tenantId);

        // Then
        assertFalse(result);
        verify(authTokenAvailableClient).getTenantTokenAvailable(tenantId, "application/json");

        // Verify error metrics
        assertEquals(1.0, meterRegistry.counter("ai.tokens.check.error.count").count());
    }

    @Test
    void testTokenAvailable_Exception() {
        // Given
        String tenantId = "123e4567-e89b-12d3-a456-426614174000";
        when(authTokenAvailableClient.getTenantTokenAvailable(tenantId, "application/json"))
                .thenThrow(new RuntimeException("Network error"));

        // When
        boolean result = tokenAvailabilityService.isTokenAvailable(tenantId);

        // Then
        assertFalse(result);
        verify(authTokenAvailableClient).getTenantTokenAvailable(tenantId, "application/json");

        // Verify error metrics
        assertEquals(1.0, meterRegistry.counter("ai.tokens.check.error.count").count());
    }

    @Test
    void testTokenAvailableWithEstimatedTokens_Available() {
        // Given
        String tenantId = "123e4567-e89b-12d3-a456-426614174000";
        int estimatedTokens = 1000;
        when(authTokenAvailableClient.getTenantTokenAvailable(tenantId, "application/json"))
                .thenReturn(ResponseEntity.ok(true));

        // When
        boolean result = tokenAvailabilityService.isTokenAvailable(tenantId, estimatedTokens);

        // Then
        assertTrue(result);
        verify(authTokenAvailableClient).getTenantTokenAvailable(tenantId, "application/json");
    }

    @Test
    void testTokenAvailableWithEstimatedTokens_NotAvailable() {
        // Given
        String tenantId = "123e4567-e89b-12d3-a456-426614174000";
        int estimatedTokens = 1000;
        when(authTokenAvailableClient.getTenantTokenAvailable(tenantId, "application/json"))
                .thenReturn(ResponseEntity.ok(false));

        // When
        boolean result = tokenAvailabilityService.isTokenAvailable(tenantId, estimatedTokens);

        // Then
        assertFalse(result);
        verify(authTokenAvailableClient).getTenantTokenAvailable(tenantId, "application/json");
    }

    @Test
    void testTokenStatusCaching() {
        // Given
        String tenantId = "123e4567-e89b-12d3-a456-426614174000";
        when(authTokenAvailableClient.getTenantTokenAvailable(tenantId, "application/json"))
                .thenReturn(ResponseEntity.ok(true));

        // When
        tokenAvailabilityService.isTokenAvailable(tenantId);
        TokenAvailabilityService.TokenStatus status = tokenAvailabilityService.getTokenStatus(tenantId);

        // Then
        assertNotNull(status);
        assertTrue(status.isAvailable());
        assertEquals("SUCCESS", status.getStatus());
        assertTrue(status.getLastChecked() > 0);
    }

    @Test
    void testCircuitBreakerFallback() {
        // This test would require a more complex setup with circuit breaker configuration
        // For now, we'll test the fallback method directly
        String tenantId = "123e4567-e89b-12d3-a456-426614174000";
        RuntimeException exception = new RuntimeException("Service unavailable");

        // When
        boolean result = tokenAvailabilityService.tokenAvailableFallback(tenantId, exception);

        // Then
        assertFalse(result);

        // Verify fallback status is cached
        TokenAvailabilityService.TokenStatus status = tokenAvailabilityService.getTokenStatus(tenantId);
        assertNotNull(status);
        assertFalse(status.isAvailable());
        assertEquals("CIRCUIT_BREAKER_FALLBACK", status.getStatus());
    }
}
