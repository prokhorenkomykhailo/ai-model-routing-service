package com.lucid.automation.airouting.pipeline.postprocessing;

import com.lucid.automation.airouting.pipeline.postprocessing.step.PipelineStepResult;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Result object for the entire pipeline execution.
 * Contains overall success status and individual step results.
 */
public class PostProcessingPipelineResult {
    
    private final boolean overallSuccess;
    private final String errorMessage;
    private final Map<String, PipelineStepResult> stepResults;
    private final LocalDateTime executionTime;
    private final long executionDurationMs;
    
    private PostProcessingPipelineResult(Builder builder) {
        this.overallSuccess = builder.overallSuccess;
        this.errorMessage = builder.errorMessage;
        this.stepResults = new HashMap<>(builder.stepResults);
        this.executionTime = LocalDateTime.now();
        this.executionDurationMs = builder.executionDurationMs;
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static PostProcessingPipelineResult success(String message) {
        return builder()
                .setOverallSuccess(true)
                .build();
    }
    
    public static PostProcessingPipelineResult failure(String errorMessage) {
        return builder()
                .setOverallSuccess(false)
                .setErrorMessage(errorMessage)
                .build();
    }
    
    // Getters
    public boolean isOverallSuccess() {
        return overallSuccess;
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }
    
    public Map<String, PipelineStepResult> getStepResults() {
        return new HashMap<>(stepResults);
    }
    
    public PipelineStepResult getStepResult(String stepName) {
        return stepResults.get(stepName);
    }
    
    public LocalDateTime getExecutionTime() {
        return executionTime;
    }
    
    public long getExecutionDurationMs() {
        return executionDurationMs;
    }
    
    public int getExecutedStepsCount() {
        return stepResults.size();
    }
    
    public int getSuccessfulStepsCount() {
        return (int) stepResults.values().stream()
                .filter(PipelineStepResult::isSuccess)
                .count();
    }
    
    public int getFailedStepsCount() {
        return (int) stepResults.values().stream()
                .filter(result -> !result.isSuccess())
                .count();
    }
    
    /**
     * Builder class for PostProcessingPipelineResult
     */
    public static class Builder {
        private boolean overallSuccess = true;
        private String errorMessage;
        private Map<String, PipelineStepResult> stepResults = new HashMap<>();
        private long executionDurationMs = 0;
        
        public Builder setOverallSuccess(boolean overallSuccess) {
            this.overallSuccess = overallSuccess;
            return this;
        }
        
        public Builder setErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }
        
        public Builder addStepResult(String stepName, PipelineStepResult result) {
            this.stepResults.put(stepName, result);
            return this;
        }
        
        public Builder setExecutionDurationMs(long executionDurationMs) {
            this.executionDurationMs = executionDurationMs;
            return this;
        }
        
        public PostProcessingPipelineResult build() {
            return new PostProcessingPipelineResult(this);
        }
    }
    
    @Override
    public String toString() {
        return "PostProcessingPipelineResult{" +
                "overallSuccess=" + overallSuccess +
                ", errorMessage='" + errorMessage + '\'' +
                ", executedSteps=" + getExecutedStepsCount() +
                ", successfulSteps=" + getSuccessfulStepsCount() +
                ", failedSteps=" + getFailedStepsCount() +
                ", executionDurationMs=" + executionDurationMs +
                '}';
    }
}
