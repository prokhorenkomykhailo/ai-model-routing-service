package com.lucid.automation.airouting.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

/**
 * Unit tests for AuditAspect
 * Tests AC-4 requirement: AOP aspect behavior and audit logging integration
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AuditAspectTest {

    @Mock
    private AuditLoggingService auditLoggingService;

    @Mock
    private AuditContextExtractor contextExtractor;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private ProceedingJoinPoint joinPoint;

    @Mock
    private MethodSignature methodSignature;

    @Mock
    private HttpServletRequest mockRequest;

    @InjectMocks
    private AuditAspect auditAspect;

    private Method testMethod;
    private Audit testAuditAnnotation;

    @BeforeEach
    void setUp() throws Exception {
        // Mock basic join point behavior
        when(joinPoint.getSignature()).thenReturn(methodSignature);

        // Create a test method with @Audit annotation
        testMethod = TestController.class.getMethod("testMethod");
        when(methodSignature.getMethod()).thenReturn(testMethod);

        // Create mock audit annotation
        testAuditAnnotation = testMethod.getAnnotation(Audit.class);

        // Mock context extractor default responses
        when(contextExtractor.getUserId()).thenReturn("test-user");
        when(contextExtractor.getTenantId()).thenReturn("test-tenant");
        when(contextExtractor.getClientIp()).thenReturn("127.0.0.1");
        when(contextExtractor.getCorrelationId()).thenReturn("test-correlation");
        when(contextExtractor.getCurrentRequest()).thenReturn(mockRequest);
        when(mockRequest.getMethod()).thenReturn("GET");
        when(mockRequest.getRequestURI()).thenReturn("/test");
    }

    @Test
    void auditMethod_withSuccessfulExecution_shouldLogSuccessAuditEvent() throws Throwable {
        // Given
        ResponseEntity<String> successResponse = ResponseEntity.ok("Success");
        when(joinPoint.proceed()).thenReturn(successResponse);

        // When
        Object result = auditAspect.auditMethod(joinPoint, testAuditAnnotation);

        // Then
        assertEquals(successResponse, result);

        // Verify audit log was called with success status
        verify(auditLoggingService).logAuditEvent(argThat(auditLog ->
            "TEST_ACTION".equals(auditLog.getAction()) &&
            "SUCCESS".equals(auditLog.getStatus()) &&
            auditLog.getResponseStatus() == 200 &&
            auditLog.getDurationMs() != null &&
            auditLog.getDurationMs() >= 0
        ));
    }

    @Test
    void auditMethod_withExceptionThrown_shouldLogFailureAuditEvent() throws Throwable {
        // Given
        RuntimeException exception = new RuntimeException("Test exception");
        when(joinPoint.proceed()).thenThrow(exception);

        // When & Then
        RuntimeException thrownException = assertThrows(RuntimeException.class, () ->
            auditAspect.auditMethod(joinPoint, testAuditAnnotation));

        assertEquals(exception, thrownException);

        // Verify audit log was called with failure status
        verify(auditLoggingService).logAuditEvent(argThat(auditLog ->
            "TEST_ACTION".equals(auditLog.getAction()) &&
            "FAILURE".equals(auditLog.getStatus()) &&
            auditLog.getDetails() != null &&
            auditLog.getDetails().contains("Test exception") &&
            auditLog.getDurationMs() != null &&
            auditLog.getDurationMs() >= 0
        ));
    }

    @Test
    void auditMethod_withNonResponseEntityReturn_shouldLogSuccess() throws Throwable {
        // Given
        String plainResult = "Plain string result";
        when(joinPoint.proceed()).thenReturn(plainResult);

        // When
        Object result = auditAspect.auditMethod(joinPoint, testAuditAnnotation);

        // Then
        assertEquals(plainResult, result);

        // Verify audit log was called with success status and null response status
        verify(auditLoggingService).logAuditEvent(argThat(auditLog ->
            "TEST_ACTION".equals(auditLog.getAction()) &&
            "SUCCESS".equals(auditLog.getStatus()) &&
            auditLog.getResponseStatus() == null
        ));
    }

    @Test
    void auditMethod_withErrorResponseEntity_shouldLogSuccessWithErrorStatus() throws Throwable {
        // Given
        ResponseEntity<String> errorResponse = ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Error");
        when(joinPoint.proceed()).thenReturn(errorResponse);

        // When
        Object result = auditAspect.auditMethod(joinPoint, testAuditAnnotation);

        // Then
        assertEquals(errorResponse, result);

        // Verify audit log was called with success status but error response code
        verify(auditLoggingService).logAuditEvent(argThat(auditLog ->
            "TEST_ACTION".equals(auditLog.getAction()) &&
            "SUCCESS".equals(auditLog.getStatus()) &&
            auditLog.getResponseStatus() == 400
        ));
    }

    @Test
    void auditMethod_shouldExtractAllContextFields() throws Throwable {
        // Given
        when(joinPoint.proceed()).thenReturn(ResponseEntity.ok("OK"));

        // When
        auditAspect.auditMethod(joinPoint, testAuditAnnotation);

        // Then
        verify(contextExtractor).getUserId();
        verify(contextExtractor).getTenantId();
        verify(contextExtractor).getClientIp();
        verify(contextExtractor).getCorrelationId();
        verify(contextExtractor).getCurrentRequest();

        // Verify audit log contains all context fields
        verify(auditLoggingService).logAuditEvent(argThat(auditLog ->
            "test-user".equals(auditLog.getUserId()) &&
            "test-tenant".equals(auditLog.getTenantId()) &&
            "127.0.0.1".equals(auditLog.getIp()) &&
            "test-correlation".equals(auditLog.getCorrelationId()) &&
            "GET".equals(auditLog.getHttpMethod()) &&
            "/test".equals(auditLog.getRequestPath())
        ));
    }

    @Test
    void auditMethod_withNullContextValues_shouldHandleGracefully() throws Throwable {
        // Given
        when(contextExtractor.getUserId()).thenReturn(null);
        when(contextExtractor.getTenantId()).thenReturn(null);
        when(contextExtractor.getClientIp()).thenReturn(null);
        when(contextExtractor.getCorrelationId()).thenReturn(null);
        when(contextExtractor.getCurrentRequest()).thenReturn(null);
        when(joinPoint.proceed()).thenReturn(ResponseEntity.ok("OK"));

        // When
        auditAspect.auditMethod(joinPoint, testAuditAnnotation);

        // Then
        // Verify audit log was still created despite null values
        verify(auditLoggingService).logAuditEvent(argThat(auditLog ->
            "TEST_ACTION".equals(auditLog.getAction()) &&
            "SUCCESS".equals(auditLog.getStatus()) &&
            auditLog.getUserId() == null &&
            auditLog.getTenantId() == null &&
            auditLog.getIp() == null &&
            auditLog.getCorrelationId() == null &&
            auditLog.getHttpMethod() == null &&
            auditLog.getRequestPath() == null
        ));
    }

    @Test
    void auditMethod_shouldMeasureExecutionTime() throws Throwable {
        // Given
        when(joinPoint.proceed()).thenAnswer(invocation -> {
            Thread.sleep(50); // Simulate some execution time
            return ResponseEntity.ok("OK");
        });

        // When
        auditAspect.auditMethod(joinPoint, testAuditAnnotation);

        // Then
        verify(auditLoggingService).logAuditEvent(argThat(auditLog ->
            auditLog.getDurationMs() != null &&
            auditLog.getDurationMs() >= 50 // At least the sleep time
        ));
    }

    @Test
    void auditMethod_withRequestBodyLogging_shouldCaptureRequestBody() throws Throwable {
        // Given
        Method methodWithRequestBodyLogging = TestController.class.getMethod("methodWithRequestBodyLogging");
        Audit auditWithRequestBody = methodWithRequestBodyLogging.getAnnotation(Audit.class);

        Object requestDto = new TestRequestDto("test data");
        when(joinPoint.getArgs()).thenReturn(new Object[]{requestDto});
        when(joinPoint.proceed()).thenReturn(ResponseEntity.ok("OK"));
        when(objectMapper.writeValueAsString(requestDto)).thenReturn("{\"data\":\"test data\"}");

        // When
        auditAspect.auditMethod(joinPoint, auditWithRequestBody);

        // Then
        verify(auditLoggingService).logAuditEvent(argThat(auditLog ->
            auditLog.getRequestBody() != null &&
            auditLog.getRequestBody().contains("test data")
        ));
    }

    @Test
    void auditMethod_withResponseBodyLogging_shouldCaptureResponseBody() throws Throwable {
        // Given
        Method methodWithResponseBodyLogging = TestController.class.getMethod("methodWithResponseBodyLogging");
        Audit auditWithResponseBody = methodWithResponseBodyLogging.getAnnotation(Audit.class);

        String responseData = "response data";
        ResponseEntity<String> response = ResponseEntity.ok(responseData);
        when(joinPoint.proceed()).thenReturn(response);
        when(objectMapper.writeValueAsString(responseData)).thenReturn("\"response data\"");

        // When
        auditAspect.auditMethod(joinPoint, auditWithResponseBody);

        // Then
        verify(auditLoggingService).logAuditEvent(argThat(auditLog ->
            auditLog.getResponseBody() != null &&
            auditLog.getResponseBody().contains("response data")
        ));
    }

    // Test controller class with various method signatures
    @SuppressWarnings("unused")
    private static class TestController {

        @Audit(action = "TEST_ACTION")
        public ResponseEntity<String> testMethod() {
            return ResponseEntity.ok("test");
        }

        @Audit(action = "REQUEST_BODY_ACTION", logRequestBody = true)
        public ResponseEntity<String> methodWithRequestBodyLogging() {
            return ResponseEntity.ok("test");
        }

        @Audit(action = "RESPONSE_BODY_ACTION", logResponseBody = true)
        public ResponseEntity<String> methodWithResponseBodyLogging() {
            return ResponseEntity.ok("test");
        }

        public String methodWithoutAudit() {
            return "no audit";
        }
    }

    // Test DTO class for request body testing
    @SuppressWarnings("unused")
    private static class TestRequestDto {
        private final String data;

        public TestRequestDto(String data) {
            this.data = data;
        }

        public String getData() {
            return data;
        }
    }
}
