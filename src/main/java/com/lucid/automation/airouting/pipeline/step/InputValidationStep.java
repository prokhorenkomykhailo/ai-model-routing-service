package com.lucid.automation.airouting.pipeline.step;

import com.lucid.automation.airouting.pipeline.context.PostProcessingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Pipeline step that validates the input data and extracts basic metadata
 */
@Component
public class InputValidationStep implements PipelineStep {
    
    private static final Logger logger = LoggerFactory.getLogger(InputValidationStep.class);
    
    @Override
    public PipelineStepResult execute(PostProcessingContext context) {
        logger.debug("Executing input validation step");
        
        try {
            Map<String, Object> responseMap = context.getRawResponseMap();
            
            // Validate basic structure
            if (responseMap == null || responseMap.isEmpty()) {
                return PipelineStepResult.failureAndSkip("Raw response map is null or empty");
            }
            
            if (!(responseMap instanceof Map<?, ?>)) {
                return PipelineStepResult.failureAndSkip(
                    "Invalid pre-AI response format: expected Map, got " + responseMap.getClass().getSimpleName());
            }
            
            // Extract basic metadata
            extractBasicMetadata(context, responseMap);
            
            // Validate required fields
            String validationError = validateRequiredFields(context);
            if (validationError != null) {
                return PipelineStepResult.failure("Validation failed: " + validationError);
            }
            
            logger.debug("Input validation completed successfully for messageId: {}", context.getMessageId());
            return PipelineStepResult.success("Input validation completed successfully");
            
        } catch (Exception e) {
            logger.error("Error during input validation: {}", e.getMessage(), e);
            return PipelineStepResult.failure("Input validation error: " + e.getMessage(), e);
        }
    }
    
    private void extractBasicMetadata(PostProcessingContext context, Map<String, Object> responseMap) {
        // Extract necessary fields from the response
        context.setMessageId((String) responseMap.get("messageId"));
        context.setCorrelationId((String) responseMap.get("correlationId"));
        context.setTaskType((String) responseMap.get("taskType"));
        context.setTenantId((String) responseMap.get("tenantId"));
        context.setTenantSchema((String) responseMap.get("tenantSchema"));
        context.setUserId((String) responseMap.get("userId"));
        context.setDeemergeUserId((String) responseMap.get("deemergeUserId"));
        context.setDeemergeUserName((String) responseMap.get("deemergeUserName"));
        context.setTeamId((String) responseMap.get("teamId"));
        
        // Extract AI result map
        @SuppressWarnings("unchecked")
        Map<String, Object> aiResultMap = (Map<String, Object>) responseMap.get("result");
        context.setAiResultMap(aiResultMap);
        
        logger.debug("Extracted metadata - messageId: {}, correlationId: {}, taskType: {}, tenantId: {}, teamId: {}", 
                   context.getMessageId(), context.getCorrelationId(), context.getTaskType(), 
                   context.getTenantId(), context.getTeamId());
    }
    
    private String validateRequiredFields(PostProcessingContext context) {
        if (context.getAiResultMap() == null) {
            return "Result map is null in pre-AI response";
        }
        
        if (context.getMessageId() == null || context.getMessageId().trim().isEmpty()) {
            return "MessageId is missing or empty";
        }
        
        // Add more validation as needed
        return null; // No validation errors
    }
    
    @Override
    public String getStepName() {
        return "InputValidation";
    }
    
    @Override
    public boolean continueOnFailure() {
        return false; // Stop processing if input validation fails
    }
    
    @Override
    public int getExecutionOrder() {
        return 10; // Execute first
    }
}
