package com.lucid.automation.airouting.pipeline.ingestion;

import com.lucid.automation.airouting.pipeline.ProcessingResult;

/**
 * Interface for message processors in the ingestion pipeline.
 * Each processor handles a specific aspect of message processing.
 * 
 * @author AI Assistant
 */
public interface MessageProcessor {
    
    /**
     * Processes a message and returns the result.
     * 
     * @param context The ingestion processing context containing the ingestion event and accumulated data
     * @return ProcessingResult indicating success/failure and any error details
     */
    ProcessingResult process(IngestionProcessingContext context);
    
    /**
     * Returns the name of this processor for logging and debugging.
     * 
     * @return The processor name
     */
    String getProcessorName();
    
    /**
     * Returns the order/priority of this processor in the pipeline.
     * Lower numbers are processed first.
     * 
     * @return The processing order
     */
    int getOrder();
}
