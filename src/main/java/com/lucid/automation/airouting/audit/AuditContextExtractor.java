package com.lucid.automation.airouting.audit;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/**
 * Utility component to extract audit context information from the current request
 * and security context. Uses MDC, security context, and OpenTelemetry for context extraction.
 */
@Component
public class AuditContextExtractor {

    private static final Logger logger = LoggerFactory.getLogger(AuditContextExtractor.class);

    private static final String UNKNOWN_USER = "unknown";
    private static final String UNKNOWN_TENANT = "unknown";
    private static final String UNKNOWN_IP = "unknown";

    // Common headers for correlation ID
    private static final String[] CORRELATION_HEADERS = {
            "X-Correlation-ID", "X-Request-ID", "X-Trace-ID", "Correlation-ID", "Request-ID"
    };

    /**
     * Extracts user ID from security context or MDC
     */
    public String getUserId() {
        try {
            // First try to get from MDC (set by authentication filters)
            String userId = MDC.get("userId");
            if (isValidId(userId)) {
                return userId;
            }

            // Try to get from Security Context
            Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
            if (authentication != null && authentication.isAuthenticated() &&
                    !"anonymousUser".equals(authentication.getName())) {
                return authentication.getName();
            }
        } catch (Exception e) {
            logger.debug("Failed to extract user ID: {}", e.getMessage());
        }

        return UNKNOWN_USER;
    }

    /**
     * Extracts tenant ID from MDC or request headers
     */
    public String getTenantId() {
        try {
            // First try MDC (set by filters)
            String tenantId = MDC.get("tenantId");
            if (isValidId(tenantId)) {
                return tenantId;
            }

            // Try to extract from request headers
            HttpServletRequest request = getCurrentRequest();
            if (request != null) {
                String headerTenant = request.getHeader("X-Tenant-ID");
                if (isValidId(headerTenant)) {
                    return headerTenant;
                }
            }
        } catch (Exception e) {
            logger.debug("Failed to extract tenant ID: {}", e.getMessage());
        }

        return UNKNOWN_TENANT;
    }

    /**
     * Extracts client IP from current HTTP request
     */
    public String getClientIp() {
        try {
            HttpServletRequest request = getCurrentRequest();
            if (request != null) {
                return extractClientIpFromRequest(request);
            }
        } catch (Exception e) {
            logger.debug("Failed to extract client IP: {}", e.getMessage());
        }

        return UNKNOWN_IP;
    }

    /**
     * Extracts correlation ID from trace context
     */
    public String getCorrelationId() {
        try {
            // First try MDC
            String correlationId = MDC.get("correlationId");
            if (isValidId(correlationId)) {
                return correlationId;
            }

            // Try request headers
            HttpServletRequest request = getCurrentRequest();
            if (request != null) {
                for (String header : CORRELATION_HEADERS) {
                    String value = request.getHeader(header);
                    if (isValidId(value)) {
                        return value;
                    }
                }
            }
        } catch (Exception e) {
            logger.debug("Failed to extract correlation ID from request headers: {}", e.getMessage());
        }

        // Fallback to OpenTelemetry
        try {
            SpanContext spanContext = Span.current().getSpanContext();
            if (spanContext.isValid()) {
                String otelTraceId = spanContext.getTraceId();
                if (isValidId(otelTraceId)) {
                    return otelTraceId;
                }
            }
        } catch (Exception e) {
            logger.debug("Failed to extract correlation ID from OpenTelemetry: {}", e.getMessage());
        }

        return null;
    }

    /**
     * Gets the current HTTP request
     */
    public HttpServletRequest getCurrentRequest() {
        try {
            ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
            return attributes != null ? attributes.getRequest() : null;
        } catch (Exception e) {
            logger.debug("Failed to get current request: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Extracts client IP from HTTP request considering forwarded headers
     */
    private String extractClientIpFromRequest(HttpServletRequest request) {
        String clientIp = request.getHeader("X-Forwarded-For");
        if (clientIp == null || clientIp.isEmpty() || UNKNOWN_IP.equalsIgnoreCase(clientIp)) {
            clientIp = request.getHeader("Proxy-Client-IP");
        }
        if (clientIp == null || clientIp.isEmpty() || UNKNOWN_IP.equalsIgnoreCase(clientIp)) {
            clientIp = request.getHeader("WL-Proxy-Client-IP");
        }
        if (clientIp == null || clientIp.isEmpty() || UNKNOWN_IP.equalsIgnoreCase(clientIp)) {
            clientIp = request.getRemoteAddr();
        }

        // If X-Forwarded-For contains multiple IPs, take the first one (client IP)
        if (clientIp != null && clientIp.contains(",")) {
            clientIp = clientIp.split(",")[0].trim();
        }

        return clientIp != null ? clientIp : UNKNOWN_IP;
    }

    /**
     * Checks if an ID is valid (not null, not empty, not all zeros)
     */
    private boolean isValidId(String id) {
        return id != null && !id.trim().isEmpty() && !isAllZeros(id);
    }

    /**
     * Checks if a trace/span ID is all zeros (invalid)
     */
    private boolean isAllZeros(String id) {
        return id == null || id.isEmpty() || id.matches("0+");
    }

    /**
     * Extracts OpenTelemetry trace ID from current span context
     */
    public String getTraceId() {
        try {
            SpanContext spanContext = Span.current().getSpanContext();
            if (spanContext.isValid()) {
                String traceId = spanContext.getTraceId();
                if (isValidId(traceId)) {
                    return traceId;
                }
            }
        } catch (Exception e) {
            logger.debug("Failed to extract trace ID from OpenTelemetry: {}", e.getMessage());
        }
        return null;
    }

    /**
     * Extracts OpenTelemetry span ID from current span context
     */
    public String getSpanId() {
        try {
            SpanContext spanContext = Span.current().getSpanContext();
            if (spanContext.isValid()) {
                String spanId = spanContext.getSpanId();
                if (isValidId(spanId)) {
                    return spanId;
                }
            }
        } catch (Exception e) {
            logger.debug("Failed to extract span ID from OpenTelemetry: {}", e.getMessage());
        }
        return null;
    }
}
