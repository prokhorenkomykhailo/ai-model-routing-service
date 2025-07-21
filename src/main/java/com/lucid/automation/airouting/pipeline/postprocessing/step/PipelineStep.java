package com.lucid.automation.airouting.pipeline.postprocessing.step;

import com.lucid.automation.airouting.pipeline.postprocessing.PostProcessingContext;

/**
 * Interface for all post-processing pipeline steps.
 * Each step performs a specific part of the post-processing workflow.
 */
public interface PipelineStep {
    
    /**
     * Execute this step of the pipeline
     * 
     * @param context The processing context containing all necessary data
     * @return PipelineStepResult indicating success/failure and any relevant information
     */
    PipelineStepResult execute(PostProcessingContext context);
    
    /**
     * Get the name of this pipeline step for logging and debugging
     * 
     * @return Step name
     */
    String getStepName();
    
    /**
     * Indicates whether this step should continue processing if it fails
     * 
     * @return true if pipeline should continue on failure, false if it should stop
     */
    default boolean continueOnFailure() {
        return false;
    }
    
    /**
     * Get the order/priority of this step when multiple steps are configured
     * Lower numbers execute first
     * 
     * @return execution order
     */
    default int getExecutionOrder() {
        return 100;
    }
}
