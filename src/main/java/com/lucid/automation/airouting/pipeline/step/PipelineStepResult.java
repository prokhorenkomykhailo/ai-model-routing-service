package com.lucid.automation.airouting.pipeline.step;

import java.util.HashMap;
import java.util.Map;

/**
 * Result object returned by pipeline steps to indicate success/failure 
 * and provide additional context or data.
 */
public class PipelineStepResult {
    
    private final boolean success;
    private final String message;
    private final Exception exception;
    private final Map<String, Object> data;
    private final boolean shouldSkipRemainingSteps;
    
    private PipelineStepResult(boolean success, String message, Exception exception, 
                              Map<String, Object> data, boolean shouldSkipRemainingSteps) {
        this.success = success;
        this.message = message;
        this.exception = exception;
        this.data = data != null ? new HashMap<>(data) : new HashMap<>();
        this.shouldSkipRemainingSteps = shouldSkipRemainingSteps;
    }
    
    /**
     * Create a successful result
     */
    public static PipelineStepResult success() {
        return new PipelineStepResult(true, null, null, null, false);
    }
    
    /**
     * Create a successful result with message
     */
    public static PipelineStepResult success(String message) {
        return new PipelineStepResult(true, message, null, null, false);
    }
    
    /**
     * Create a successful result with data
     */
    public static PipelineStepResult success(Map<String, Object> data) {
        return new PipelineStepResult(true, null, null, data, false);
    }
    
    /**
     * Create a successful result with message and data
     */
    public static PipelineStepResult success(String message, Map<String, Object> data) {
        return new PipelineStepResult(true, message, null, data, false);
    }
    
    /**
     * Create a failure result
     */
    public static PipelineStepResult failure(String message) {
        return new PipelineStepResult(false, message, null, null, false);
    }
    
    /**
     * Create a failure result with exception
     */
    public static PipelineStepResult failure(String message, Exception exception) {
        return new PipelineStepResult(false, message, exception, null, false);
    }
    
    /**
     * Create a failure result that should skip remaining steps
     */
    public static PipelineStepResult failureAndSkip(String message) {
        return new PipelineStepResult(false, message, null, null, true);
    }
    
    /**
     * Create a failure result with exception that should skip remaining steps
     */
    public static PipelineStepResult failureAndSkip(String message, Exception exception) {
        return new PipelineStepResult(false, message, exception, null, true);
    }
    
    /**
     * Create a successful result that should skip remaining steps (early completion)
     */
    public static PipelineStepResult successAndSkip(String message) {
        return new PipelineStepResult(true, message, null, null, true);
    }
    
    // Getters
    public boolean isSuccess() {
        return success;
    }
    
    public String getMessage() {
        return message;
    }
    
    public Exception getException() {
        return exception;
    }
    
    public Map<String, Object> getData() {
        return new HashMap<>(data);
    }
    
    public Object getData(String key) {
        return data.get(key);
    }
    
    public boolean shouldSkipRemainingSteps() {
        return shouldSkipRemainingSteps;
    }
    
    @Override
    public String toString() {
        return "PipelineStepResult{" +
                "success=" + success +
                ", message='" + message + '\'' +
                ", exception=" + (exception != null ? exception.getClass().getSimpleName() : null) +
                ", hasData=" + !data.isEmpty() +
                ", shouldSkipRemainingSteps=" + shouldSkipRemainingSteps +
                '}';
    }
}
