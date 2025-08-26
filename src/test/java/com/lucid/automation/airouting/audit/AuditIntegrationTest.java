package com.lucid.automation.airouting.audit;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Integration tests for the complete audit system
 * Tests AC-4 requirement: Complete audit system integration including JSON structure
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AuditIntegrationTest {

    @Mock
    private HttpServletRequest mockRequest;

    @Mock
    private ServletRequestAttributes mockRequestAttributes;

    @Mock
    private SecurityContext mockSecurityContext;

    @Mock
    private Authentication mockAuthentication;

    private ObjectMapper objectMapper;
    private AuditContextExtractor contextExtractor;
    private AuditLoggingService auditLoggingService;

    private ListAppender<ILoggingEvent> listAppender;
    private Logger auditLogger;

    @BeforeEach
    void setUp() {
        // Clear MDC before each test
        MDC.clear();

        // Set up real components (no mocking for integration test)
        objectMapper = new ObjectMapper();
        contextExtractor = new AuditContextExtractor();
        auditLoggingService = new AuditLoggingService(objectMapper);

        // Set up logback test appender for AUDIT logger
        auditLogger = (Logger) LoggerFactory.getLogger("AUDIT");
        listAppender = new ListAppender<>();
        listAppender.start();
        auditLogger.addAppender(listAppender);

        // Mock request setup
        when(mockRequestAttributes.getRequest()).thenReturn(mockRequest);
        when(mockRequest.getMethod()).thenReturn("POST");
        when(mockRequest.getRequestURI()).thenReturn("/api/ai/text-query");
        when(mockRequest.getHeader("X-Forwarded-For")).thenReturn("192.168.1.100");
        when(mockRequest.getHeader("X-Correlation-ID")).thenReturn("test-correlation-123");
        when(mockRequest.getHeader("X-Tenant-ID")).thenReturn("test-tenant-456");

        // Mock security context
        when(mockAuthentication.getName()).thenReturn("test-user-789");
        when(mockAuthentication.isAuthenticated()).thenReturn(true);
        when(mockSecurityContext.getAuthentication()).thenReturn(mockAuthentication);
    }

    @Test
    void completeAuditFlow_shouldProduceCorrectAuditEventJSON() throws Exception {
        // Given - Set up complete audit context
        MDC.put("userId", "mdc-user-123");
        MDC.put("tenantId", "mdc-tenant-456");
        MDC.put("correlationId", "mdc-correlation-789");

        // When - Execute complete audit flow
        try (MockedStatic<RequestContextHolder> requestContextHolder = mockStatic(RequestContextHolder.class);
             MockedStatic<SecurityContextHolder> securityContextHolder = mockStatic(SecurityContextHolder.class)) {

            requestContextHolder.when(RequestContextHolder::getRequestAttributes).thenReturn(mockRequestAttributes);
            securityContextHolder.when(SecurityContextHolder::getContext).thenReturn(mockSecurityContext);

            // Create audit log using real context extractor
            AuditLog auditLog = AuditLog.builder()
                    .action("AI_TEXT_QUERY")
                    .status("SUCCESS")
                    .details("Text query processed successfully")
                    .userId(contextExtractor.getUserId())
                    .tenantId(contextExtractor.getTenantId())
                    .ip(contextExtractor.getClientIp())
                    .correlationId(contextExtractor.getCorrelationId())
                    .httpMethod("POST")
                    .requestPath("/api/ai/text-query")
                    .responseStatus(200)
                    .durationMs(125L)
                    .build();

            // Execute audit logging
            auditLoggingService.logAuditEvent(auditLog);
        }

        // Then - Verify complete audit JSON structure
        assertEquals(1, listAppender.list.size());
        ILoggingEvent loggingEvent = listAppender.list.get(0);

        // Extract and parse the JSON from the log message
        String logMessage = loggingEvent.getMessage();
        assertTrue(logMessage.contains("AUDIT_EVENT"));

        // Extract JSON part (after "AUDIT_EVENT: ")
        String jsonPart = logMessage.substring(logMessage.indexOf("{"));
        JsonNode auditJson = objectMapper.readTree(jsonPart);

        // Verify all required PRD fields are present
        assertNotNull(auditJson.get("timestamp"), "timestamp field is required");
        assertEquals("mdc-user-123", auditJson.get("userId").asText(), "userId should come from MDC");
        assertEquals("mdc-tenant-456", auditJson.get("tenantId").asText(), "tenantId should come from MDC");
        assertEquals("192.168.1.100", auditJson.get("ip").asText(), "ip should be extracted from X-Forwarded-For");
        assertEquals("mdc-correlation-789", auditJson.get("correlationId").asText(), "correlationId should come from MDC");

        // Verify action-specific fields
        assertEquals("AI_TEXT_QUERY", auditJson.get("action").asText());
        assertEquals("SUCCESS", auditJson.get("status").asText());
        assertEquals("Text query processed successfully", auditJson.get("details").asText());
        assertEquals("POST", auditJson.get("httpMethod").asText());
        assertEquals("/api/ai/text-query", auditJson.get("requestPath").asText());
        assertEquals(200, auditJson.get("responseStatus").asInt());
        assertEquals(125, auditJson.get("durationMs").asLong());

        // Verify attributes contain audit marker
        JsonNode attributes = auditJson.get("attributes");
        assertNotNull(attributes, "attributes should be present");
        assertTrue(attributes.get("audit").asBoolean(), "audit attribute should be true");
    }

    @Test
    void auditWithFallbackValues_shouldUseDefaultsWhenContextUnavailable() throws Exception {
        // Given - No MDC context, no security context, no request context
        MDC.clear();

        try (MockedStatic<RequestContextHolder> requestContextHolder = mockStatic(RequestContextHolder.class);
             MockedStatic<SecurityContextHolder> securityContextHolder = mockStatic(SecurityContextHolder.class)) {

            requestContextHolder.when(RequestContextHolder::getRequestAttributes).thenReturn(null);
            securityContextHolder.when(SecurityContextHolder::getContext).thenReturn(null);

            // When - Create audit log with fallback values
            AuditLog auditLog = AuditLog.builder()
                    .action("FALLBACK_TEST")
                    .status("SUCCESS")
                    .userId(contextExtractor.getUserId())
                    .tenantId(contextExtractor.getTenantId())
                    .ip(contextExtractor.getClientIp())
                    .correlationId(contextExtractor.getCorrelationId())
                    .build();

            auditLoggingService.logAuditEvent(auditLog);

            // Then - Verify fallback values are used
            assertEquals(1, listAppender.list.size());
            ILoggingEvent loggingEvent = listAppender.list.get(0);

            String logMessage = loggingEvent.getMessage();
            String jsonPart = logMessage.substring(logMessage.indexOf("{"));
            JsonNode auditJson = objectMapper.readTree(jsonPart);

            assertEquals("unknown", auditJson.get("userId").asText());
            assertEquals("unknown", auditJson.get("tenantId").asText());
            assertEquals("unknown", auditJson.get("ip").asText());
            assertTrue(auditJson.get("correlationId").isNull() || auditJson.get("correlationId").asText().isEmpty());
        }
    }

    @Test
    void auditLogging_whenAuditDisabled_shouldNotProduceLogEntry() throws Exception {
        // Given - Disable audit logging
        java.lang.reflect.Field auditEnabledField = AuditLoggingService.class.getDeclaredField("auditEnabled");
        auditEnabledField.setAccessible(true);
        auditEnabledField.set(auditLoggingService, false);

        AuditLog auditLog = AuditLog.builder()
                .action("DISABLED_TEST")
                .status("SUCCESS")
                .build();

        // When
        auditLoggingService.logAuditEvent(auditLog);

        // Then - No log should be produced
        assertEquals(0, listAppender.list.size());
    }

    @Test
    void auditJSON_shouldContainAllRequiredPRDFields() throws Exception {
        // Given - Complete audit context from different sources
        MDC.put("userId", "mdc-user");

        try (MockedStatic<RequestContextHolder> requestContextHolder = mockStatic(RequestContextHolder.class);
             MockedStatic<SecurityContextHolder> securityContextHolder = mockStatic(SecurityContextHolder.class)) {

            // Setup mixed context sources (MDC + headers + security)
            requestContextHolder.when(RequestContextHolder::getRequestAttributes).thenReturn(mockRequestAttributes);
            securityContextHolder.when(SecurityContextHolder::getContext).thenReturn(mockSecurityContext);

            when(mockRequest.getHeader("X-Tenant-ID")).thenReturn("header-tenant");
            when(mockRequest.getHeader("X-Correlation-ID")).thenReturn("header-correlation");

            // When
            AuditLog auditLog = AuditLog.builder()
                    .action("PRD_COMPLIANCE_TEST")
                    .status("SUCCESS")
                    .details("Testing PRD field requirements")
                    .userId(contextExtractor.getUserId())
                    .tenantId(contextExtractor.getTenantId())
                    .ip(contextExtractor.getClientIp())
                    .correlationId(contextExtractor.getCorrelationId())
                    .httpMethod("GET")
                    .requestPath("/api/test")
                    .responseStatus(200)
                    .durationMs(50L)
                    .build();

            auditLoggingService.logAuditEvent(auditLog);

            // Then - Verify all PRD required fields are present and properly sourced
            assertEquals(1, listAppender.list.size());
            ILoggingEvent loggingEvent = listAppender.list.get(0);

            String jsonPart = loggingEvent.getMessage().substring(loggingEvent.getMessage().indexOf("{"));
            JsonNode auditJson = objectMapper.readTree(jsonPart);

            // PRD AC-1: Required fields must be present
            assertNotNull(auditJson.get("timestamp"), "AC-1: timestamp is required");
            assertNotNull(auditJson.get("userId"), "AC-1: userId is required");
            assertNotNull(auditJson.get("tenantId"), "AC-1: tenantId is required");
            assertNotNull(auditJson.get("ip"), "AC-1: ip is required");
            assertNotNull(auditJson.get("correlationId"), "AC-1: correlationId is required");

            // PRD AC-2: Values should come from appropriate sources
            assertEquals("mdc-user", auditJson.get("userId").asText(), "AC-2: userId from MDC preferred");
            assertEquals("header-tenant", auditJson.get("tenantId").asText(), "AC-2: tenantId from headers");
            assertEquals("192.168.1.100", auditJson.get("ip").asText(), "AC-2: ip from X-Forwarded-For");
            assertEquals("header-correlation", auditJson.get("correlationId").asText(), "AC-2: correlationId from headers");

            // PRD AC-3: Audit marker in attributes
            JsonNode attributes = auditJson.get("attributes");
            assertNotNull(attributes, "AC-3: attributes field required");
            assertTrue(attributes.get("audit").asBoolean(), "AC-3: audit=true marker required");
        }
    }

    @Test
    void auditJSONStructure_shouldMatchExpectedFormat() throws Exception {
        // Given
        AuditLog auditLog = AuditLog.builder()
                .action("JSON_STRUCTURE_TEST")
                .status("SUCCESS")
                .details("Testing JSON structure compliance")
                .userId("test-user")
                .tenantId("test-tenant")
                .ip("127.0.0.1")
                .correlationId("test-corr-id")
                .httpMethod("POST")
                .requestPath("/api/json-test")
                .responseStatus(201)
                .durationMs(75L)
                .requestBody("{\"input\":\"test\"}")
                .responseBody("{\"result\":\"success\"}")
                .build();

        // When
        auditLoggingService.logAuditEvent(auditLog);

        // Then - Verify complete JSON structure
        assertEquals(1, listAppender.list.size());
        ILoggingEvent loggingEvent = listAppender.list.get(0);

        String jsonPart = loggingEvent.getMessage().substring(loggingEvent.getMessage().indexOf("{"));
        JsonNode auditJson = objectMapper.readTree(jsonPart);

        // Verify all expected fields exist with correct types
        assertTrue(auditJson.has("timestamp"), "timestamp field missing");
        assertTrue(auditJson.has("userId"), "userId field missing");
        assertTrue(auditJson.has("tenantId"), "tenantId field missing");
        assertTrue(auditJson.has("ip"), "ip field missing");
        assertTrue(auditJson.has("action"), "action field missing");
        assertTrue(auditJson.has("status"), "status field missing");
        assertTrue(auditJson.has("details"), "details field missing");
        assertTrue(auditJson.has("correlationId"), "correlationId field missing");
        assertTrue(auditJson.has("httpMethod"), "httpMethod field missing");
        assertTrue(auditJson.has("requestPath"), "requestPath field missing");
        assertTrue(auditJson.has("responseStatus"), "responseStatus field missing");
        assertTrue(auditJson.has("durationMs"), "durationMs field missing");
        assertTrue(auditJson.has("requestBody"), "requestBody field missing");
        assertTrue(auditJson.has("responseBody"), "responseBody field missing");
        assertTrue(auditJson.has("attributes"), "attributes field missing");

        // Verify field values and types
        assertTrue(auditJson.get("timestamp").isTextual());
        assertEquals("test-user", auditJson.get("userId").asText());
        assertEquals("test-tenant", auditJson.get("tenantId").asText());
        assertEquals("127.0.0.1", auditJson.get("ip").asText());
        assertEquals("JSON_STRUCTURE_TEST", auditJson.get("action").asText());
        assertEquals("SUCCESS", auditJson.get("status").asText());
        assertEquals("Testing JSON structure compliance", auditJson.get("details").asText());
        assertEquals("test-corr-id", auditJson.get("correlationId").asText());
        assertEquals("POST", auditJson.get("httpMethod").asText());
        assertEquals("/api/json-test", auditJson.get("requestPath").asText());
        assertEquals(201, auditJson.get("responseStatus").asInt());
        assertEquals(75, auditJson.get("durationMs").asLong());
        assertEquals("{\"input\":\"test\"}", auditJson.get("requestBody").asText());
        assertEquals("{\"result\":\"success\"}", auditJson.get("responseBody").asText());

        JsonNode attributes = auditJson.get("attributes");
        assertTrue(attributes.get("audit").asBoolean());
    }
}
