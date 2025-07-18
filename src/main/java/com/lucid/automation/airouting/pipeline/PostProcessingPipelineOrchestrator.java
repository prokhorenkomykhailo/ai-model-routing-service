package com.lucid.automation.airouting.pipeline;

import com.lucid.automation.airouting.pipeline.context.PostProcessingContext;
import com.lucid.automation.airouting.pipeline.step.PipelineStep;
import com.lucid.automation.airouting.pipeline.step.PipelineStepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Orchestrates the execution of pipeline steps for post-processing.
 * Manages step ordering, error handling, and overall pipeline execution flow.
 */
@Component
public class PostProcessingPipelineOrchestrator {
    
    private static final Logger logger = LoggerFactory.getLogger(PostProcessingPipelineOrchestrator.class);
    
    /**
     * Execute a pipeline with the given steps
     * 
     * @param context The processing context
     * @param steps List of pipeline steps to execute
     * @return PostProcessingPipelineResult containing overall execution results
     */
    public PostProcessingPipelineResult execute(PostProcessingContext context, List<PipelineStep> steps) {
        if (context == null) {
            logger.error("Pipeline execution failed: context is null");
            return PostProcessingPipelineResult.failure("Context is null");
        }
        
        if (steps == null || steps.isEmpty()) {
            logger.warn("Pipeline execution completed: no steps to execute");
            return PostProcessingPipelineResult.success("No steps to execute");
        }
        
        logger.info("Starting pipeline execution with {} steps for messageId: {}", 
                   steps.size(), context.getMessageId());
        
        // Sort steps by execution order
        List<PipelineStep> sortedSteps = new ArrayList<>(steps);
        sortedSteps.sort(Comparator.comparingInt(PipelineStep::getExecutionOrder));
        
        PostProcessingPipelineResult.Builder resultBuilder = PostProcessingPipelineResult.builder();
        int executedSteps = 0;
        
        for (PipelineStep step : sortedSteps) {
            String stepName = step.getStepName();
            logger.debug("Executing pipeline step: {}", stepName);
            
            try {
                long startTime = System.currentTimeMillis();
                PipelineStepResult stepResult = step.execute(context);
                long executionTime = System.currentTimeMillis() - startTime;
                
                executedSteps++;
                resultBuilder.addStepResult(stepName, stepResult);
                
                logger.debug("Step '{}' completed in {}ms: {}", stepName, executionTime, stepResult);
                
                // Check if we should stop processing
                if (!stepResult.isSuccess()) {
                    if (step.continueOnFailure()) {
                        logger.warn("Step '{}' failed but continueOnFailure=true, continuing pipeline", stepName);
                    } else {
                        logger.error("Step '{}' failed and continueOnFailure=false, stopping pipeline", stepName);
                        resultBuilder.setOverallSuccess(false);
                        resultBuilder.setErrorMessage("Pipeline stopped due to step failure: " + stepName);
                        break;
                    }
                }
                
                // Check if step requests to skip remaining steps
                if (stepResult.shouldSkipRemainingSteps()) {
                    logger.info("Step '{}' requested to skip remaining steps", stepName);
                    break;
                }
                
            } catch (Exception e) {
                logger.error("Unexpected error in pipeline step '{}': {}", stepName, e.getMessage(), e);
                executedSteps++;
                
                PipelineStepResult errorResult = PipelineStepResult.failure(
                    "Unexpected error: " + e.getMessage(), e);
                resultBuilder.addStepResult(stepName, errorResult);
                
                if (!step.continueOnFailure()) {
                    resultBuilder.setOverallSuccess(false);
                    resultBuilder.setErrorMessage("Pipeline stopped due to unexpected error in step: " + stepName);
                    break;
                }
            }
        }
        
        PostProcessingPipelineResult result = resultBuilder.build();
        
        logger.info("Pipeline execution completed. Executed {}/{} steps. Success: {}. MessageId: {}", 
                   executedSteps, sortedSteps.size(), result.isOverallSuccess(), context.getMessageId());
        
        if (!result.isOverallSuccess()) {
            logger.error("Pipeline execution failed for messageId {}: {}", 
                        context.getMessageId(), result.getErrorMessage());
        }
        
        return result;
    }
    
    /**
     * Execute a single pipeline step
     * 
     * @param context The processing context
     * @param step The pipeline step to execute
     * @return PostProcessingPipelineResult containing execution results
     */
    public PostProcessingPipelineResult executeSingleStep(PostProcessingContext context, PipelineStep step) {
        return execute(context, List.of(step));
    }
}
