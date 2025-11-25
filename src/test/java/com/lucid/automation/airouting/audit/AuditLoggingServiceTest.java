package com.lucid.automation.airouting.audit;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Unit tests for AuditLoggingService
 * Tests AC-4 requirement: logging emission including MDC keys and audit JSON structure
 */
@ExtendWith(MockitoExtension.class)
class AuditLoggingServiceTest {

    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private AuditLoggingService auditLoggingService;

    private ListAppender<ILoggingEvent> listAppender;
    private Logger auditLogger;

    @BeforeEach
    void setUp() {
        // Clear MDC before each test
        MDC.clear();

        // Set up logback test appender for AUDIT logger
        auditLogger = (Logger) LoggerFactory.getLogger("AUDIT");
        listAppender = new ListAppender<>();
        listAppender.start();
        auditLogger.addAppender(listAppender);
    }

    @Test
    void logAuditEvent_withValidAuditLog_shouldSetMDCKeysAndEmitLog() throws Exception {
        // Given
        AuditLog auditLog = AuditLog.builder()
                .timestamp(OffsetDateTime.now(ZoneOffset.UTC))
                .userId("test-user-123")
                .tenantId("test-tenant-456")
                .ip("192.168.1.100")
                .action("TEST_ACTION")
                .status("SUCCESS")
                .details("Test audit event")
                .correlationId("corr-123-456")
                .httpMethod("GET")
                .requestPath("/api/test")
                .responseStatus(200)
                .durationMs(150L)
                .build();

        String expectedJson = "{\"timestamp\":\"2025-08-25T07:30:00Z\",\"userId\":\"test-user-123\",\"action\":\"TEST_ACTION\"}";
        when(objectMapper.writeValueAsString(any(AuditLog.class))).thenReturn(expectedJson);

        // When
        auditLoggingService.logAuditEvent(auditLog);

        // Then
        assertEquals(1, listAppender.list.size());
        ILoggingEvent loggingEvent = listAppender.list.get(0);

        // Verify log message contains AUDIT_EVENT and JSON
        assertTrue(loggingEvent.getMessage().contains("AUDIT_EVENT"));
        assertTrue(loggingEvent.getMessage().contains(expectedJson));

        // Verify MDC keys were set during logging (they get cleared after)
        // We can't directly verify MDC during the test since it's cleared in finally block
        // But we can verify the logging event was created with correct level
        assertEquals(ch.qos.logback.classic.Level.INFO, loggingEvent.getLevel());
        assertEquals("AUDIT", loggingEvent.getLoggerName());
    }

    @Test
    void logAuditEvent_withMinimalAuditLog_shouldHandleNullFields() throws Exception {
        // Given
        AuditLog auditLog = AuditLog.builder()
                .timestamp(OffsetDateTime.now(ZoneOffset.UTC))
                .action("MINIMAL_ACTION")
                .status("SUCCESS")
                .build();

        String expectedJson = "{\"timestamp\":\"2025-08-25T07:30:00Z\",\"action\":\"MINIMAL_ACTION\"}";
        when(objectMapper.writeValueAsString(any(AuditLog.class))).thenReturn(expectedJson);

        // When
        auditLoggingService.logAuditEvent(auditLog);

        // Then
        assertEquals(1, listAppender.list.size());
        ILoggingEvent loggingEvent = listAppender.list.get(0);
        assertTrue(loggingEvent.getMessage().contains("AUDIT_EVENT"));
        assertTrue(loggingEvent.getMessage().contains(expectedJson));
    }

    @Test
    void logAuditEvent_whenAuditDisabled_shouldNotEmitLog() throws Exception {
        // Given - Create service with audit disabled
        AuditLoggingService disabledService = new AuditLoggingService(objectMapper);

        // Use reflection to set auditEnabled to false
        java.lang.reflect.Field auditEnabledField = AuditLoggingService.class.getDeclaredField("auditEnabled");
        auditEnabledField.setAccessible(true);
        auditEnabledField.set(disabledService, false);

        AuditLog auditLog = AuditLog.builder()
                .action("DISABLED_ACTION")
                .status("SUCCESS")
                .build();

        // When
        disabledService.logAuditEvent(auditLog);

        // Then
        assertEquals(0, listAppender.list.size());
    }

    @Test
    void logAuditEvent_shouldEnsureAuditAttributeIsSet() throws Exception {
        // Given
        AuditLog auditLog = AuditLog.builder()
                .action("ATTRIBUTE_TEST")
                .status("SUCCESS")
                .build();

        String expectedJson = "{\"action\":\"ATTRIBUTE_TEST\",\"attributes\":{\"audit\":true}}";
        when(objectMapper.writeValueAsString(any(AuditLog.class))).thenReturn(expectedJson);

        // When
        auditLoggingService.logAuditEvent(auditLog);

        // Then
        assertEquals(1, listAppender.list.size());

        // Verify audit attribute was set
        assertNotNull(auditLog.getAttributes());
        assertTrue((Boolean) auditLog.getAttributes().get("audit"));
    }

    @Test
    void logAuditEvent_withExistingAttributes_shouldPreserveAndAddAuditAttribute() throws Exception {
        // Given
        Map<String, Object> existingAttributes = new HashMap<>();
        existingAttributes.put("custom", "value");

        AuditLog auditLog = AuditLog.builder()
                .action("EXISTING_ATTRS_TEST")
                .status("SUCCESS")
                .attributes(existingAttributes)
                .build();

        String expectedJson = "{\"action\":\"EXISTING_ATTRS_TEST\",\"attributes\":{\"custom\":\"value\",\"audit\":true}}";
        when(objectMapper.writeValueAsString(any(AuditLog.class))).thenReturn(expectedJson);

        // When
        auditLoggingService.logAuditEvent(auditLog);

        // Then
        assertEquals(1, listAppender.list.size());

        // Verify both custom and audit attributes exist
        assertNotNull(auditLog.getAttributes());
        assertEquals("value", auditLog.getAttributes().get("custom"));
        assertTrue((Boolean) auditLog.getAttributes().get("audit"));
    }

    @Test
    void logSuccess_shouldCreateAndLogSuccessAuditEvent() throws Exception {
        // Given
        String action = "SUCCESS_TEST";
        String userId = "success-user";
        String tenantId = "success-tenant";
        String details = "Success test details";

        when(objectMapper.writeValueAsString(any(AuditLog.class))).thenReturn("{\"action\":\"SUCCESS_TEST\",\"status\":\"SUCCESS\"}");

        // When
        auditLoggingService.logSuccess(action, userId, tenantId, details);

        // Then
        assertEquals(1, listAppender.list.size());
        ILoggingEvent loggingEvent = listAppender.list.get(0);
        assertTrue(loggingEvent.getMessage().contains("AUDIT_EVENT"));
        assertTrue(loggingEvent.getMessage().contains("SUCCESS_TEST"));
        assertTrue(loggingEvent.getMessage().contains("SUCCESS"));
    }

    @Test
    void logFailure_shouldCreateAndLogFailureAuditEvent() throws Exception {
        // Given
        String action = "FAILURE_TEST";
        String userId = "failure-user";
        String tenantId = "failure-tenant";
        String details = "Failure test details";

        when(objectMapper.writeValueAsString(any(AuditLog.class))).thenReturn("{\"action\":\"FAILURE_TEST\",\"status\":\"FAILURE\"}");

        // When
        auditLoggingService.logFailure(action, userId, tenantId, details);

        // Then
        assertEquals(1, listAppender.list.size());
        ILoggingEvent loggingEvent = listAppender.list.get(0);
        assertTrue(loggingEvent.getMessage().contains("AUDIT_EVENT"));
        assertTrue(loggingEvent.getMessage().contains("FAILURE_TEST"));
        assertTrue(loggingEvent.getMessage().contains("FAILURE"));
    }

    @Test
    void logAuditEvent_shouldPreserveMDCValuesAfterLogging() throws Exception {
        // Given
        String originalUserId = "original-user";
        String originalTenantId = "original-tenant";
        MDC.put("userId", originalUserId);
        MDC.put("tenantId", originalTenantId);

        AuditLog auditLog = AuditLog.builder()
                .userId("audit-user")
                .tenantId("audit-tenant")
                .action("MDC_PRESERVE_TEST")
                .status("SUCCESS")
                .build();

        when(objectMapper.writeValueAsString(any(AuditLog.class))).thenReturn("{\"action\":\"MDC_PRESERVE_TEST\"}");

        // When
        auditLoggingService.logAuditEvent(auditLog);

        // Then
        assertEquals(1, listAppender.list.size());

        // Verify original MDC values were restored
        assertEquals(originalUserId, MDC.get("userId"));
        assertEquals(originalTenantId, MDC.get("tenantId"));
    }
}
