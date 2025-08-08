package com.lucid.automation.airouting.logging;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

@Component
public class MdcLoggingFilter extends OncePerRequestFilter {

    private static final Logger logger = LoggerFactory.getLogger(MdcLoggingFilter.class);
    private static final String SERVICE_NAME = "ai-routing-service";
    private static final String UNKNOWN_IP = "unknown";

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {

        try {
            // Generate unique request ID if not present
            String requestId = getOrGenerateRequestId(request);
            MDC.put(LoggingConstants.REQUEST_ID, requestId);

            // Add service name
            MDC.put(LoggingConstants.SERVICE, SERVICE_NAME);

            // Get client IP (considering forwarded headers)
            String clientIp = extractClientIp(request);
            MDC.put(LoggingConstants.CLIENT_IP, clientIp);

            // Add request path and method
            MDC.put(LoggingConstants.REQUEST_PATH, request.getRequestURI());
            MDC.put(LoggingConstants.REQUEST_METHOD, request.getMethod());

            // Get current user if authenticated
            Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
            if (authentication != null && authentication.isAuthenticated() &&
                    !"anonymousUser".equals(authentication.getPrincipal().toString())) {
                MDC.put(LoggingConstants.USER_ID, authentication.getName());
            }

            // Get trace and span IDs from headers (injected by gateway)
            String traceId = request.getHeader("X-Trace-Id");
            String spanId = request.getHeader("X-Span-Id");

            if (isValidId(traceId)) {
                MDC.put(LoggingConstants.TRACE_ID, traceId);
            }
            if (isValidId(spanId)) {
                MDC.put(LoggingConstants.SPAN_ID, spanId);
            }

            // Extract tenant information from headers
            String tenantId = request.getHeader("X-Tenant-Id");
            if (tenantId != null && !tenantId.trim().isEmpty()) {
                MDC.put(LoggingConstants.TENANT_ID, tenantId);
            }

            // Log request start
            logger.info("Request received: {} {}", request.getMethod(), request.getRequestURI());

            // Continue with the request
            filterChain.doFilter(request, response);
        } finally {
            // Log request completion
            logger.info("Request completed with status {}", response.getStatus());

            // Clear MDC context
            MDC.clear();
        }
    }

    /**
     * Get request ID from headers or generate new one
     */
    private String getOrGenerateRequestId(HttpServletRequest request) {
        String requestId = request.getHeader("X-Request-Id");
        if (requestId != null && !requestId.trim().isEmpty()) {
            return requestId;
        }

        // Check alternative headers
        String[] headers = {"X-Request-ID", "X-Correlation-ID"};
        for (String header : headers) {
            String value = request.getHeader(header);
            if (value != null && !value.trim().isEmpty()) {
                return value;
            }
        }

        return UUID.randomUUID().toString();
    }

    private String extractClientIp(HttpServletRequest request) {
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

        return clientIp;
    }

    /**
     * Check if an ID is valid (not null, not empty, not all zeros)
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
}
