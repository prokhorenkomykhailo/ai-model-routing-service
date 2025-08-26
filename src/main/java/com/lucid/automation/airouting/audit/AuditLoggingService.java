package com.lucid.automation.airouting.audit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/**
 * Service responsible for sending audit logs to CloudWatch via OpenTelemetry.
 * Creates structured logs that can be filtered and routed specifically to CloudWatch.
 */
@Service
public class AuditLoggingService {

    private static final Logger logger = LoggerFactory.getLogger(AuditLoggingService.class);

    // Use a dedicated audit logger that can be configured separately
    private static final Logger auditLogger = LoggerFactory.getLogger("AUDIT");

    private final ObjectMapper objectMapper;

    @Value("${lucid.audit.enabled:true}")
    private boolean auditEnabled;

    public AuditLoggingService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Logs an audit event using structured logging via SLF4J.
     * The AUDIT logger can be configured to route to CloudWatch via OpenTelemetry collector.
     */
    public void logAuditEvent(AuditLog auditLog) {
        // Check if audit logging is enabled
        if (!auditEnabled) {
            logger.debug("Audit logging is disabled, skipping audit event for action: {}", auditLog.getAction());
            return;
        }

        try {
            // Ensure audit attribute is set for downstream identification
            ensureAuditAttributes(auditLog);

            // Set up MDC with audit context
            String originalUserId = MDC.get("userId");
            String originalTenantId = MDC.get("tenantId");
            String originalAction = MDC.get("auditAction");
            String originalStatus = MDC.get("auditStatus");

            try {
                // Add audit-specific MDC keys
                if (auditLog.getUserId() != null) {
                    MDC.put("userId", auditLog.getUserId());
                }
                if (auditLog.getTenantId() != null) {
                    MDC.put("tenantId", auditLog.getTenantId());
                }
                MDC.put("auditAction", auditLog.getAction());
                MDC.put("auditStatus", auditLog.getStatus());
                MDC.put("logType", "audit");

                if (auditLog.getIp() != null) {
                    MDC.put("clientIp", auditLog.getIp());
                }
                if (auditLog.getCorrelationId() != null) {
                    MDC.put("correlationId", auditLog.getCorrelationId());
                }
                if (auditLog.getTraceId() != null) {
                    MDC.put("traceId", auditLog.getTraceId());
                }
                if (auditLog.getSpanId() != null) {
                    MDC.put("spanId", auditLog.getSpanId());
                }
                if (auditLog.getHttpMethod() != null) {
                    MDC.put("httpMethod", auditLog.getHttpMethod());
                }
                if (auditLog.getRequestPath() != null) {
                    MDC.put("httpPath", auditLog.getRequestPath());
                }
                if (auditLog.getResponseStatus() != null) {
                    MDC.put("httpStatus", auditLog.getResponseStatus().toString());
                }
                if (auditLog.getDurationMs() != null) {
                    MDC.put("durationMs", auditLog.getDurationMs().toString());
                }

                // Convert audit log to JSON for structured logging
                String auditLogJson = objectMapper.writeValueAsString(auditLog);

                // Log the audit event
                auditLogger.info("AUDIT_EVENT: {}", auditLogJson);

                logger.debug("Audit log emitted successfully for action: {} by user: {}",
                            auditLog.getAction(), auditLog.getUserId());

            } finally {
                // Restore original MDC values
                if (originalUserId != null) {
                    MDC.put("userId", originalUserId);
                } else {
                    MDC.remove("userId");
                }
                if (originalTenantId != null) {
                    MDC.put("tenantId", originalTenantId);
                } else {
                    MDC.remove("tenantId");
                }
                if (originalAction != null) {
                    MDC.put("auditAction", originalAction);
                } else {
                    MDC.remove("auditAction");
                }
                if (originalStatus != null) {
                    MDC.put("auditStatus", originalStatus);
                } else {
                    MDC.remove("auditStatus");
                }

                // Clean up audit-specific MDC keys
                MDC.remove("logType");
                MDC.remove("clientIp");
                MDC.remove("correlationId");
                MDC.remove("traceId");
                MDC.remove("spanId");
                MDC.remove("httpMethod");
                MDC.remove("httpPath");
                MDC.remove("httpStatus");
                MDC.remove("durationMs");
            }

        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize audit log to JSON", e);
        } catch (Exception e) {
            logger.error("Failed to emit audit log", e);
        }
    }

    /**
     * Creates a simple audit log for success scenarios
     */
    public void logSuccess(String action, String userId, String tenantId, String details) {
        AuditLog auditLog = AuditLog.builder()
                .timestamp(OffsetDateTime.now(ZoneOffset.UTC))
                .userId(userId)
                .tenantId(tenantId)
                .action(action)
                .status("SUCCESS")
                .details(details)
                .build();
        logAuditEvent(auditLog);
    }

    /**
     * Creates a simple audit log for failure scenarios
     */
    public void logFailure(String action, String userId, String tenantId, String details) {
        AuditLog auditLog = AuditLog.builder()
                .timestamp(OffsetDateTime.now(ZoneOffset.UTC))
                .userId(userId)
                .tenantId(tenantId)
                .action(action)
                .status("FAILURE")
                .details(details)
                .build();
        logAuditEvent(auditLog);
    }

    /**
     * Ensures that the audit log has the required attributes for downstream identification.
     * Sets attributes["audit"] = true if not already present.
     */
    private void ensureAuditAttributes(AuditLog auditLog) {
        if (auditLog.getAttributes() == null) {
            auditLog.setAttributes(new java.util.HashMap<>());
        }

        // Ensure audit attribute is set to true for downstream system identification
        auditLog.getAttributes().put("audit", true);

        logger.debug("Set audit attribute for action: {} by user: {}",
                    auditLog.getAction(), auditLog.getUserId());
    }
}
