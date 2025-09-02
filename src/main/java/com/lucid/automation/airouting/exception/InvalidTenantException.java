package com.lucid.automation.airouting.exception;

/**
 * Exception thrown when an AI request is made with an invalid tenant ID.
 * This exception indicates that the request should be rejected due to tenant validation failure.
 */
public class InvalidTenantException extends RuntimeException {

    private final String tenantId;
    private final String reason;

    public InvalidTenantException(String tenantId, String reason) {
        super(String.format("Invalid tenant ID '%s': %s", tenantId, reason));
        this.tenantId = tenantId;
        this.reason = reason;
    }

    public InvalidTenantException(String tenantId, String reason, Throwable cause) {
        super(String.format("Invalid tenant ID '%s': %s", tenantId, reason), cause);
        this.tenantId = tenantId;
        this.reason = reason;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getReason() {
        return reason;
    }
}
