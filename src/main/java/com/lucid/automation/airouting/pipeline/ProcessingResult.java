package com.lucid.automation.airouting.pipeline;

/**
 * Result of processing a message through a processor.
 * Contains success/failure status and any error information.
 * 
 * @author AI Assistant
 */
public class ProcessingResult {
    
    private final boolean success;
    private final String errorMessage;
    private final Exception exception;
    private final String processorName;
    
    private ProcessingResult(boolean success, String errorMessage, Exception exception, String processorName) {
        this.success = success;
        this.errorMessage = errorMessage;
        this.exception = exception;
        this.processorName = processorName;
    }
    
    /**
     * Creates a successful processing result.
     * 
     * @param processorName The name of the processor that generated this result
     * @return A successful ProcessingResult
     */
    public static ProcessingResult success(String processorName) {
        return new ProcessingResult(true, null, null, processorName);
    }
    
    /**
     * Creates a failed processing result with an error message.
     * 
     * @param processorName The name of the processor that generated this result
     * @param errorMessage The error message
     * @return A failed ProcessingResult
     */
    public static ProcessingResult failure(String processorName, String errorMessage) {
        return new ProcessingResult(false, errorMessage, null, processorName);
    }
    
    /**
     * Creates a failed processing result with an exception.
     * 
     * @param processorName The name of the processor that generated this result
     * @param exception The exception that caused the failure
     * @return A failed ProcessingResult
     */
    public static ProcessingResult failure(String processorName, Exception exception) {
        return new ProcessingResult(false, exception.getMessage(), exception, processorName);
    }
    
    /**
     * Creates a failed processing result with both message and exception.
     * 
     * @param processorName The name of the processor that generated this result
     * @param errorMessage The error message
     * @param exception The exception that caused the failure
     * @return A failed ProcessingResult
     */
    public static ProcessingResult failure(String processorName, String errorMessage, Exception exception) {
        return new ProcessingResult(false, errorMessage, exception, processorName);
    }
    
    public boolean isSuccess() {
        return success;
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }
    
    public Exception getException() {
        return exception;
    }
    
    public String getProcessorName() {
        return processorName;
    }
    
    @Override
    public String toString() {
        return String.format("ProcessingResult{processor='%s', success=%s, error='%s'}", 
                           processorName, success, errorMessage);
    }
}
