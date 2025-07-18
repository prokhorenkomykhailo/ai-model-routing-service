package com.lucid.automation.airouting.pipeline.step;

import com.lucid.automation.common.dto.enrichment.EnrichmentResponse;
import com.lucid.automation.airouting.pipeline.context.PostProcessingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Pipeline step that builds the final EnrichmentResponse object
 */
@Component
public class ResponseBuildingStep implements PipelineStep {
    
    private static final Logger logger = LoggerFactory.getLogger(ResponseBuildingStep.class);
    private static final String SUCCESS_STATUS = "success";
    
    @Override
    public PipelineStepResult execute(PostProcessingContext context) {
        logger.debug("Executing response building step");
        
        try {
            // Create a new response object with the processed data
            EnrichmentResponse enrichmentResponse = new EnrichmentResponse();
            enrichmentResponse.setMessageId(context.getMessageId());
            enrichmentResponse.setCorrelationId(context.getCorrelationId());
            enrichmentResponse.setTaskType(context.getTaskType());
            enrichmentResponse.setSuccess(true);
            enrichmentResponse.setStatus(SUCCESS_STATUS);
            enrichmentResponse.setResult(context.getConversationEnrichment());
            
            // Convert processedAt to the expected array format
            LocalDateTime processedAt = context.getProcessedAt();
            if (processedAt != null) {
                enrichmentResponse.setProcessedAt(List.of(
                    processedAt.getYear(),
                    processedAt.getMonthValue(),
                    processedAt.getDayOfMonth(),
                    processedAt.getHour(),
                    processedAt.getMinute(),
                    processedAt.getSecond()
                ));
            } else {
                LocalDateTime now = LocalDateTime.now();
                enrichmentResponse.setProcessedAt(List.of(
                    now.getYear(),
                    now.getMonthValue(),
                    now.getDayOfMonth(),
                    now.getHour(),
                    now.getMinute(),
                    now.getSecond()
                ));
            }
            
            enrichmentResponse.setTenantId(context.getTenantId());
            enrichmentResponse.setTenantSchema(context.getTenantSchema());
            enrichmentResponse.setUserId(context.getUserId());
            enrichmentResponse.setDeemergeUserId(context.getDeemergeUserId());
            enrichmentResponse.setDeemergeUserName(context.getDeemergeUserName());
            enrichmentResponse.setTeamId(context.getTeamId());
            
            context.setEnrichmentResponse(enrichmentResponse);
            
            logger.debug("Response building completed successfully for messageId: {}", context.getMessageId());
            return PipelineStepResult.success("Response building completed successfully");
            
        } catch (Exception e) {
            logger.error("Error during response building: {}", e.getMessage(), e);
            return PipelineStepResult.failure("Response building error: " + e.getMessage(), e);
        }
    }
    
    @Override
    public String getStepName() {
        return "ResponseBuilding";
    }
    
    @Override
    public boolean continueOnFailure() {
        return false; // Stop processing if response building fails
    }
    
    @Override
    public int getExecutionOrder() {
        return 90; // Execute near the end
    }
}
