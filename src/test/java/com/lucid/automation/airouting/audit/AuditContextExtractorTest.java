package com.lucid.automation.airouting.audit;

import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for AuditContextExtractor
 * Tests AC-4 requirement: extractor behavior with unknown defaults
 */
@ExtendWith(MockitoExtension.class)
class AuditContextExtractorTest {

    @InjectMocks
    private AuditContextExtractor auditContextExtractor;

    @BeforeEach
    void setUp() {
        // Clear MDC before each test
        MDC.clear();

        // Clear SecurityContext
        SecurityContextHolder.clearContext();

        // Clear RequestContextHolder
        RequestContextHolder.resetRequestAttributes();
    }

    @Test
    void getUserId_whenNoContext_shouldReturnUnknown() {
        // When
        String userId = auditContextExtractor.getUserId();

        // Then
        assertEquals("unknown", userId);
    }

    @Test
    void getUserId_whenMDCExists_shouldReturnMDCValue() {
        // Given
        String expectedUserId = "test-user-123";
        MDC.put("userId", expectedUserId);

        // When
        String userId = auditContextExtractor.getUserId();

        // Then
        assertEquals(expectedUserId, userId);
    }

    @Test
    void getUserId_whenSecurityContextExists_shouldReturnAuthenticatedUser() {
        // Given
        String expectedUserId = "authenticated-user";
        Authentication authentication = mock(Authentication.class);
        SecurityContext securityContext = mock(SecurityContext.class);

        when(securityContext.getAuthentication()).thenReturn(authentication);
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn(expectedUserId);

        SecurityContextHolder.setContext(securityContext);

        // When
        String userId = auditContextExtractor.getUserId();

        // Then
        assertEquals(expectedUserId, userId);
    }

    @Test
    void getUserId_whenAnonymousUser_shouldReturnUnknown() {
        // Given
        Authentication authentication = mock(Authentication.class);
        SecurityContext securityContext = mock(SecurityContext.class);

        when(securityContext.getAuthentication()).thenReturn(authentication);
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("anonymousUser");

        SecurityContextHolder.setContext(securityContext);

        // When
        String userId = auditContextExtractor.getUserId();

        // Then
        assertEquals("unknown", userId);
    }

    @Test
    void getTenantId_whenNoContext_shouldReturnUnknown() {
        // When
        String tenantId = auditContextExtractor.getTenantId();

        // Then
        assertEquals("unknown", tenantId);
    }

    @Test
    void getTenantId_whenMDCExists_shouldReturnMDCValue() {
        // Given
        String expectedTenantId = "tenant-456";
        MDC.put("tenantId", expectedTenantId);

        // When
        String tenantId = auditContextExtractor.getTenantId();

        // Then
        assertEquals(expectedTenantId, tenantId);
    }

    @Test
    void getTenantId_whenHttpHeaderExists_shouldReturnHeaderValue() {
        // Given
        String expectedTenantId = "header-tenant-789";
        HttpServletRequest request = mock(HttpServletRequest.class);
        ServletRequestAttributes attributes = mock(ServletRequestAttributes.class);

        when(attributes.getRequest()).thenReturn(request);
        when(request.getHeader("X-Tenant-ID")).thenReturn(expectedTenantId);

        try (MockedStatic<RequestContextHolder> mockedRequestContext = mockStatic(RequestContextHolder.class)) {
            mockedRequestContext.when(RequestContextHolder::getRequestAttributes).thenReturn(attributes);

            // When
            String tenantId = auditContextExtractor.getTenantId();

            // Then
            assertEquals(expectedTenantId, tenantId);
        }
    }

    @Test
    void getClientIp_whenNoRequest_shouldReturnUnknown() {
        // When
        String clientIp = auditContextExtractor.getClientIp();

        // Then
        assertEquals("unknown", clientIp);
    }

    @Test
    void getClientIp_whenXForwardedForExists_shouldReturnFirstIp() {
        // Given
        String expectedIp = "192.168.1.100";
        String xForwardedFor = expectedIp + ", 10.0.0.1, 172.16.0.1";
        HttpServletRequest request = mock(HttpServletRequest.class);
        ServletRequestAttributes attributes = mock(ServletRequestAttributes.class);

        when(attributes.getRequest()).thenReturn(request);
        when(request.getHeader("X-Forwarded-For")).thenReturn(xForwardedFor);

        try (MockedStatic<RequestContextHolder> mockedRequestContext = mockStatic(RequestContextHolder.class)) {
            mockedRequestContext.when(RequestContextHolder::getRequestAttributes).thenReturn(attributes);

            // When
            String clientIp = auditContextExtractor.getClientIp();

            // Then
            assertEquals(expectedIp, clientIp);
        }
    }

    @Test
    void getClientIp_whenOnlyRemoteAddrAvailable_shouldReturnRemoteAddr() {
        // Given
        String expectedIp = "203.0.113.1";
        HttpServletRequest request = mock(HttpServletRequest.class);
        ServletRequestAttributes attributes = mock(ServletRequestAttributes.class);

        when(attributes.getRequest()).thenReturn(request);
        when(request.getHeader("X-Forwarded-For")).thenReturn(null);
        when(request.getHeader("Proxy-Client-IP")).thenReturn(null);
        when(request.getHeader("WL-Proxy-Client-IP")).thenReturn(null);
        when(request.getRemoteAddr()).thenReturn(expectedIp);

        try (MockedStatic<RequestContextHolder> mockedRequestContext = mockStatic(RequestContextHolder.class)) {
            mockedRequestContext.when(RequestContextHolder::getRequestAttributes).thenReturn(attributes);

            // When
            String clientIp = auditContextExtractor.getClientIp();

            // Then
            assertEquals(expectedIp, clientIp);
        }
    }

    @Test
    void getCorrelationId_whenMDCExists_shouldReturnMDCValue() {
        // Given
        String expectedCorrelationId = "corr-123-456";
        MDC.put("correlationId", expectedCorrelationId);

        // When
        String correlationId = auditContextExtractor.getCorrelationId();

        // Then
        assertEquals(expectedCorrelationId, correlationId);
    }

    @Test
    void getCorrelationId_whenHttpHeaderExists_shouldReturnHeaderValue() {
        // Given
        String expectedCorrelationId = "header-corr-789";
        HttpServletRequest request = mock(HttpServletRequest.class);
        ServletRequestAttributes attributes = mock(ServletRequestAttributes.class);

        when(attributes.getRequest()).thenReturn(request);
        when(request.getHeader("X-Correlation-ID")).thenReturn(expectedCorrelationId);

        try (MockedStatic<RequestContextHolder> mockedRequestContext = mockStatic(RequestContextHolder.class)) {
            mockedRequestContext.when(RequestContextHolder::getRequestAttributes).thenReturn(attributes);

            // When
            String correlationId = auditContextExtractor.getCorrelationId();

            // Then
            assertEquals(expectedCorrelationId, correlationId);
        }
    }

    @Test
    void getCorrelationId_whenNoContext_shouldReturnNull() {
        // When
        String correlationId = auditContextExtractor.getCorrelationId();

        // Then
        assertNull(correlationId);
    }

    @Test
    void getCurrentRequest_whenNoRequestContext_shouldReturnNull() {
        // When
        HttpServletRequest request = auditContextExtractor.getCurrentRequest();

        // Then
        assertNull(request);
    }

    @Test
    void getCurrentRequest_whenRequestContextExists_shouldReturnRequest() {
        // Given
        HttpServletRequest expectedRequest = mock(HttpServletRequest.class);
        ServletRequestAttributes attributes = mock(ServletRequestAttributes.class);

        when(attributes.getRequest()).thenReturn(expectedRequest);

        try (MockedStatic<RequestContextHolder> mockedRequestContext = mockStatic(RequestContextHolder.class)) {
            mockedRequestContext.when(RequestContextHolder::getRequestAttributes).thenReturn(attributes);

            // When
            HttpServletRequest request = auditContextExtractor.getCurrentRequest();

            // Then
            assertEquals(expectedRequest, request);
        }
    }
}
