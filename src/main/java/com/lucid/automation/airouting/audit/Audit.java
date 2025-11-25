package com.lucid.automation.airouting.audit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to mark methods for audit logging.
 * When applied to a method, it will automatically log audit information to CloudWatch
 * including user context, action details, and request/response data.
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Audit {

    /**
     * The action being performed (e.g., "AI_TEXT_QUERY", "AI_MESSAGE_ROUTE", "AI_PIPELINE_EXECUTE").
     * This is required and will be used to categorize the audit event.
     */
    String action();

    /**
     * Optional human-readable description of the audit event.
     * Provides additional context about what the operation does.
     */
    String description() default "";

    /**
     * Whether to include the request body in the audit log.
     * Use with caution for endpoints containing sensitive data.
     */
    boolean logRequestBody() default false;

    /**
     * Whether to include the response body in the audit log.
     * Use with caution for endpoints returning sensitive data.
     */
    boolean logResponseBody() default false;
}
