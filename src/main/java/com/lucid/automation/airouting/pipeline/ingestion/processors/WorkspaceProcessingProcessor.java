package com.lucid.automation.airouting.pipeline.ingestion.processors;

import com.lucid.automation.airouting.pipeline.ingestion.MessageProcessor;
import com.lucid.automation.airouting.pipeline.ingestion.IngestionProcessingContext;
import com.lucid.automation.airouting.pipeline.ProcessingResult;
import com.lucid.automation.airouting.service.WorkspaceService;
import com.lucid.automation.airouting.model.Workspace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Processes workspace information from ingestion messages.
 * Creates or updates workspace records based on the ingestion event.
 * 
 * @author AI Assistant
 */
@Component
public class WorkspaceProcessingProcessor implements MessageProcessor {
    
    private static final Logger logger = LoggerFactory.getLogger(WorkspaceProcessingProcessor.class);
    
    private final WorkspaceService workspaceService;
    
    public WorkspaceProcessingProcessor(WorkspaceService workspaceService) {
        this.workspaceService = workspaceService;
    }
    
    @Override
    public ProcessingResult process(IngestionProcessingContext context) {
        var ingestionEvent = context.getIngestionEvent();
        
        logger.debug("Processing workspace information for messageId: {}, tenantId: {}", 
            ingestionEvent.getMessage().getTs(), ingestionEvent.getTenantId());
        
        try {
            // Process workspace information
            Workspace workspace = workspaceService.createOrUpdateWorkspace(ingestionEvent);
            
            if (workspace == null) {
                logger.warn("Workspace service returned null for tenantId: {}", ingestionEvent.getTenantId());
                // This is not a critical failure, so we continue
                context.setWorkspaceProcessed(false);
                context.setProcessingData("workspaceWarning", "Workspace service returned null");
            } else {
                logger.debug("Workspace processed successfully: {}", workspace.getName());
                context.setWorkspaceProcessed(true);
                context.setProcessedWorkspace(workspace);
            }
            
            return ProcessingResult.success(getProcessorName());
            
        } catch (Exception e) {
            String errorMsg = "Exception while processing workspace: " + e.getMessage();
            logger.error(errorMsg, e);
            
            // Workspace processing failure is not critical, so we continue
            context.setWorkspaceProcessed(false);
            context.setProcessingData("workspaceError", errorMsg);
            
            return ProcessingResult.success(getProcessorName()); // Return success to continue pipeline
        }
    }
    
    @Override
    public String getProcessorName() {
        return "WorkspaceProcessingProcessor";
    }
    
    @Override
    public int getOrder() {
        return 40; // Fourth processor, after user processing
    }
}
