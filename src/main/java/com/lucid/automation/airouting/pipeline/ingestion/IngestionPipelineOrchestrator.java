package com.lucid.automation.airouting.pipeline.ingestion;

import com.lucid.automation.common.dto.messaging.IngestionEventDTO;
import com.lucid.automation.airouting.pipeline.ingestion.processors.MessageStorageProcessor;
import com.lucid.automation.airouting.pipeline.ingestion.processors.ValidationProcessor;
import com.lucid.automation.airouting.pipeline.ProcessingResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * Orchestrates the ingestion pipeline by executing processors in order.
 * This replaces the monolithic processing logic in IngestionConsumer.
 * 
 * @author AI Assistant
 */
@Service
public class IngestionPipelineOrchestrator {
    
    private static final Logger logger = LoggerFactory.getLogger(IngestionPipelineOrchestrator.class);
    
    private final List<MessageProcessor> processors;
    
    public IngestionPipelineOrchestrator(List<MessageProcessor> processors) {
        this.processors = new ArrayList<>(processors);
        // Sort processors by order
        this.processors.sort(Comparator.comparingInt(MessageProcessor::getOrder));
        logger.info("Initialized IngestionPipelineOrchestrator with {} processors: {}", 
            this.processors.size(),
            this.processors.stream().map(MessageProcessor::getProcessorName).toList());
    }
    
    /**
     * Processes an ingestion event through the pipeline.
     * 
     * @param ingestionEvent The event to process
     * @return ProcessingResult indicating overall success/failure
     */
    public ProcessingResult processMessage(IngestionEventDTO ingestionEvent) {
        IngestionProcessingContext context = new IngestionProcessingContext(ingestionEvent);
        
        logger.debug("Starting ingestion pipeline for messageId: {}, tenantId: {}", 
            context.getMessageId(), context.getTenantId());
        
        boolean overallSuccess = true;
        String failureReason = null;
        
        for (MessageProcessor processor : processors) {
            try {
                logger.debug("Executing processor: {}", processor.getProcessorName());
                ProcessingResult result = processor.process(context);
                
                if (!result.isSuccess()) {
                    logger.error("Processor {} failed: {}", processor.getProcessorName(), result.getErrorMessage());
                    overallSuccess = false;
                    failureReason = result.getErrorMessage();
                    
                    // For critical processors, stop the pipeline
                    if (processor instanceof ValidationProcessor || 
                        processor instanceof MessageStorageProcessor) {
                        logger.error("Critical processor failed, stopping pipeline");
                        break;
                    }
                }
                
                logger.debug("Processor {} completed successfully", processor.getProcessorName());
                
            } catch (Exception e) {
                logger.error("Exception in processor {}: {}", processor.getProcessorName(), e.getMessage(), e);
                overallSuccess = false;
                failureReason = "Exception in " + processor.getProcessorName() + ": " + e.getMessage();
                
                // For critical processors, stop the pipeline
                if (processor instanceof ValidationProcessor || 
                    processor instanceof MessageStorageProcessor) {
                    logger.error("Critical processor failed with exception, stopping pipeline");
                    break;
                }
            }
        }
        
        if (overallSuccess) {
            logger.info("Ingestion pipeline completed successfully for messageId: {}", context.getMessageId());
            return ProcessingResult.success("IngestionPipelineOrchestrator");
        } else {
            logger.error("Ingestion pipeline failed for messageId: {}, reason: {}", 
                context.getMessageId(), failureReason);
            return ProcessingResult.failure("IngestionPipelineOrchestrator", failureReason);
        }
    }
}
