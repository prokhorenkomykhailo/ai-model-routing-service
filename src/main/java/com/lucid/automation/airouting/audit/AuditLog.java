package com.lucid.automation.airouting.audit;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;
import java.util.Map;

/**
 * Represents an audit log entry that will be sent to CloudWatch.
 * Contains all necessary context about the audited action.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AuditLog {

    /**
     * Timestamp when the action occurred (UTC)
     */
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'")
    private OffsetDateTime timestamp;

    /**
     * ID of the user performing the action
     */
    private String userId;

    /**
     * ID of the tenant context
     */
    private String tenantId;

    /**
     * IP address of the client
     */
    private String ip;

    /**
     * The action being performed (from @Audit annotation)
     */
    private String action;

    /**
     * Status of the operation (SUCCESS, FAILURE, ERROR)
     */
    private String status;

    /**
     * Description of the action or error details
     */
    private String details;

    /**
     * Correlation ID for tracing requests across services
     */
    private String correlationId;

    /**
     * OpenTelemetry trace ID
     */
    private String traceId;

    /**
     * OpenTelemetry span ID
     */
    private String spanId;

    /**
     * HTTP method of the request
     */
    private String httpMethod;

    /**
     * Request path/endpoint
     */
    private String requestPath;

    /**
     * HTTP response status code
     */
    private Integer responseStatus;

    /**
     * Duration of the operation in milliseconds
     */
    private Long durationMs;

    /**
     * Request body (if logRequestBody = true in annotation)
     */
    private String requestBody;

    /**
     * Response body (if logResponseBody = true in annotation)
     */
    private String responseBody;

    /**
     * Additional custom attributes
     */
    private Map<String, Object> attributes;
}
