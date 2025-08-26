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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for TokenAvailabilityService
 */
@ExtendWith(MockitoExtension.class)
class TokenAvailabilityServiceTest {

    @Mock
    private AuthTokenAvailableClient authTokenAvailableClient;

    private MeterRegistry meterRegistry;
    private TokenAvailabilityService tokenAvailabilityService;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        tokenAvailabilityService = new TokenAvailabilityService(authTokenAvailableClient, meterRegistry);
    }

    @Test
    void testIsTokenAvailable_Success_True() {
        // Given
        String tenantId = "tenant-123";
        ResponseEntity<Boolean> mockResponse = ResponseEntity.ok(true);
        when(authTokenAvailableClient.getTenantTokenAvailable(eq(tenantId), any())).thenReturn(mockResponse);

        // When
        boolean result = tokenAvailabilityService.isTokenAvailable(tenantId);

        // Then
        assertTrue(result);
        verify(authTokenAvailableClient).getTenantTokenAvailable(tenantId, "application/json");

        // Check that token status is cached
        TokenAvailabilityService.TokenStatus status = tokenAvailabilityService.getTokenStatus(tenantId);
        assertNotNull(status);
        assertTrue(status.isAvailable());
        assertEquals("SUCCESS", status.getStatus());
    }

    @Test
    void testIsTokenAvailable_Success_False() {
        // Given
        String tenantId = "tenant-123";
        ResponseEntity<Boolean> mockResponse = ResponseEntity.ok(false);
        when(authTokenAvailableClient.getTenantTokenAvailable(eq(tenantId), any())).thenReturn(mockResponse);

        // When
        boolean result = tokenAvailabilityService.isTokenAvailable(tenantId);

        // Then
        assertFalse(result);
        verify(authTokenAvailableClient).getTenantTokenAvailable(tenantId, "application/json");

        // Check that token status is cached
        TokenAvailabilityService.TokenStatus status = tokenAvailabilityService.getTokenStatus(tenantId);
        assertNotNull(status);
        assertFalse(status.isAvailable());
        assertEquals("SUCCESS", status.getStatus());
    }

    @Test
    void testIsTokenAvailable_HttpError() {
        // Given
        String tenantId = "tenant-123";
        ResponseEntity<Boolean> mockResponse = ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        when(authTokenAvailableClient.getTenantTokenAvailable(eq(tenantId), any())).thenReturn(mockResponse);

        // When
        boolean result = tokenAvailabilityService.isTokenAvailable(tenantId);

        // Then
        assertFalse(result);
        verify(authTokenAvailableClient).getTenantTokenAvailable(tenantId, "application/json");

        // Check that error status is cached
        TokenAvailabilityService.TokenStatus status = tokenAvailabilityService.getTokenStatus(tenantId);
        assertNotNull(status);
        assertFalse(status.isAvailable());
        assertEquals("HTTP_ERROR", status.getStatus());
    }

    @Test
    void testIsTokenAvailable_Exception() {
        // Given
        String tenantId = "tenant-123";
        when(authTokenAvailableClient.getTenantTokenAvailable(eq(tenantId), any()))
            .thenThrow(new RuntimeException("Connection timeout"));

        // When
        boolean result = tokenAvailabilityService.isTokenAvailable(tenantId);

        // Then
        assertFalse(result);
        verify(authTokenAvailableClient).getTenantTokenAvailable(tenantId, "application/json");

        // Check that error status is cached
        TokenAvailabilityService.TokenStatus status = tokenAvailabilityService.getTokenStatus(tenantId);
        assertNotNull(status);
        assertFalse(status.isAvailable());
        assertEquals("EXCEPTION", status.getStatus());
    }

    @Test
    void testIsTokenAvailable_WithEstimatedTokens() {
        // Given
        String tenantId = "tenant-123";
        int estimatedTokens = 100;
        ResponseEntity<Boolean> mockResponse = ResponseEntity.ok(true);
        when(authTokenAvailableClient.getTenantTokenAvailable(eq(tenantId), any())).thenReturn(mockResponse);

        // When
        boolean result = tokenAvailabilityService.isTokenAvailable(tenantId, estimatedTokens);

        // Then
        assertTrue(result);
        verify(authTokenAvailableClient).getTenantTokenAvailable(tenantId, "application/json");
    }

    @Test
    void testTokenAvailableFallback() {
        // Given
        String tenantId = "tenant-123";
        Throwable throwable = new RuntimeException("Circuit breaker open");

        // When
        boolean result = tokenAvailabilityService.tokenAvailableFallback(tenantId, throwable);

        // Then
        assertFalse(result);

        // Check that fallback status is cached
        TokenAvailabilityService.TokenStatus status = tokenAvailabilityService.getTokenStatus(tenantId);
        assertNotNull(status);
        assertFalse(status.isAvailable());
        assertEquals("CIRCUIT_BREAKER_FALLBACK", status.getStatus());
    }

    @Test
    void testGetAllTokenStatuses() {
        // Given
        String tenantId1 = "tenant-123";
        String tenantId2 = "tenant-456";

        ResponseEntity<Boolean> mockResponse1 = ResponseEntity.ok(true);
        ResponseEntity<Boolean> mockResponse2 = ResponseEntity.ok(false);

        when(authTokenAvailableClient.getTenantTokenAvailable(eq(tenantId1), any())).thenReturn(mockResponse1);
        when(authTokenAvailableClient.getTenantTokenAvailable(eq(tenantId2), any())).thenReturn(mockResponse2);

        // When
        tokenAvailabilityService.isTokenAvailable(tenantId1);
        tokenAvailabilityService.isTokenAvailable(tenantId2);

        var allStatuses = tokenAvailabilityService.getAllTokenStatuses();

        // Then
        assertEquals(2, allStatuses.size());
        assertTrue(allStatuses.containsKey(tenantId1));
        assertTrue(allStatuses.containsKey(tenantId2));
        assertTrue(allStatuses.get(tenantId1).isAvailable());
        assertFalse(allStatuses.get(tenantId2).isAvailable());
    }

    @Test
    void testTokenStatus_ToString() {
        // Given
        TokenAvailabilityService.TokenStatus status =
            new TokenAvailabilityService.TokenStatus(true, "SUCCESS", 1234567890L);

        // When
        String result = status.toString();

        // Then
        assertEquals("TokenStatus{available=true, status='SUCCESS', lastChecked=1234567890}", result);
    }
}
