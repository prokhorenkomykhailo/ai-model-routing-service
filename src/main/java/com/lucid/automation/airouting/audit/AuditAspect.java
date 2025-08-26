package com.lucid.automation.airouting.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import jakarta.servlet.http.HttpServletRequest;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/**
 * AOP Aspect that intercepts methods annotated with @Audit and automatically
 * logs audit information to CloudWatch via OpenTelemetry.
 */
@Aspect
@Component
public class AuditAspect {

    private static final Logger logger = LoggerFactory.getLogger(AuditAspect.class);

    private final AuditLoggingService auditLoggingService;
    private final AuditContextExtractor contextExtractor;
    private final ObjectMapper objectMapper;

    public AuditAspect(AuditLoggingService auditLoggingService,
                      AuditContextExtractor contextExtractor,
                      ObjectMapper objectMapper) {
        this.auditLoggingService = auditLoggingService;
        this.contextExtractor = contextExtractor;
        this.objectMapper = objectMapper;
    }

    /**
     * Intercepts all methods annotated with @Audit
     */
    @Around("@annotation(audit)")
    public Object auditMethod(ProceedingJoinPoint joinPoint, Audit audit) throws Throwable {
        long startTime = System.currentTimeMillis();
        OffsetDateTime timestamp = OffsetDateTime.now(ZoneOffset.UTC);

        // Extract audit context
        String userId = contextExtractor.getUserId();
        String tenantId = contextExtractor.getTenantId();
        String clientIp = contextExtractor.getClientIp();
        String correlationId = contextExtractor.getCorrelationId();
        String traceId = contextExtractor.getTraceId();
        String spanId = contextExtractor.getSpanId();

        // Extract HTTP request information
        HttpServletRequest request = contextExtractor.getCurrentRequest();
        String httpMethod = null;
        String requestPath = null;
        String requestBody = null;

        if (request != null) {
            httpMethod = request.getMethod();
            requestPath = request.getRequestURI();

            // Capture request body if requested
            if (audit.logRequestBody()) {
                requestBody = extractRequestBody(joinPoint);
            }
        }

        Object result = null;
        String status = "SUCCESS";
        String details = audit.description();
        String responseBody = null;
        Integer responseStatus = null;
        Exception thrownException = null;

        try {
            // Execute the actual method
            result = joinPoint.proceed();

            // Extract response information
            if (result instanceof ResponseEntity<?> responseEntity) {
                responseStatus = responseEntity.getStatusCode().value();

                // Capture response body if requested
                if (audit.logResponseBody()) {
                    responseBody = extractResponseBody(responseEntity.getBody());
                }
            }

        } catch (Exception e) {
            thrownException = e;
            status = "FAILURE";
            details = audit.description() + " - Error: " + e.getMessage();
            logger.debug("Exception caught in audit aspect for action {}: {}", audit.action(), e.getMessage());
        }

        // Calculate duration
        long duration = System.currentTimeMillis() - startTime;

        // Create and log audit event
        AuditLog auditLog = AuditLog.builder()
                .timestamp(timestamp)
                .userId(userId)
                .tenantId(tenantId)
                .ip(clientIp)
                .action(audit.action())
                .status(status)
                .details(details)
                .correlationId(correlationId)
                .traceId(traceId)
                .spanId(spanId)
                .httpMethod(httpMethod)
                .requestPath(requestPath)
                .responseStatus(responseStatus)
                .durationMs(duration)
                .requestBody(requestBody)
                .responseBody(responseBody)
                .build();

        try {
            auditLoggingService.logAuditEvent(auditLog);
        } catch (Exception e) {
            logger.error("Failed to log audit event for action: {}", audit.action(), e);
        }

        // Re-throw exception if one occurred
        if (thrownException != null) {
            throw thrownException;
        }

        return result;
    }

    /**
     * Extracts request body from method parameters
     */
    private String extractRequestBody(ProceedingJoinPoint joinPoint) {
        try {
            Object[] args = joinPoint.getArgs();
            if (args != null && args.length > 0) {
                // Look for the request body parameter (usually the last non-primitive parameter)
                for (Object arg : args) {
                    if (arg != null && !isPrimitiveOrWrapper(arg.getClass())) {
                        return objectMapper.writeValueAsString(arg);
                    }
                }
            }
        } catch (Exception e) {
            logger.debug("Failed to extract request body: {}", e.getMessage());
        }
        return null;
    }

    /**
     * Extracts response body as JSON string
     */
    private String extractResponseBody(Object responseBody) {
        try {
            if (responseBody != null) {
                return objectMapper.writeValueAsString(responseBody);
            }
        } catch (Exception e) {
            logger.debug("Failed to extract response body: {}", e.getMessage());
        }
        return null;
    }

    /**
     * Checks if a class is a primitive or wrapper type
     */
    private boolean isPrimitiveOrWrapper(Class<?> clazz) {
        return clazz.isPrimitive() ||
               clazz == Boolean.class || clazz == Byte.class || clazz == Character.class ||
               clazz == Double.class || clazz == Float.class || clazz == Integer.class ||
               clazz == Long.class || clazz == Short.class || clazz == String.class;
    }
}
