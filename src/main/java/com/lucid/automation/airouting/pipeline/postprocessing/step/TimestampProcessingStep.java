package com.lucid.automation.airouting.pipeline.postprocessing.step;

import com.lucid.automation.airouting.pipeline.postprocessing.PostProcessingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * Pipeline step that processes timestamps in various formats
 */
@Component
public class TimestampProcessingStep implements PipelineStep {
    
    private static final Logger logger = LoggerFactory.getLogger(TimestampProcessingStep.class);
    
    @Override
    public PipelineStepResult execute(PostProcessingContext context) {
        logger.debug("Executing timestamp processing step");
        
        try {
            Map<String, Object> responseMap = context.getRawResponseMap();
            
            // Handle processedAt field - it could be an array or a string
            Object processedAtObj = responseMap.get("processedAt");
            LocalDateTime processedAtTime = parseTimestamp(processedAtObj);
            
            if (processedAtTime == null) {
                processedAtTime = LocalDateTime.now();
                logger.debug("Using current time as processedAt: {}", processedAtTime);
            }
            
            context.setProcessedAt(processedAtTime);
            
            logger.debug("Timestamp processing completed successfully. ProcessedAt: {}", processedAtTime);
            return PipelineStepResult.success("Timestamp processing completed successfully");
            
        } catch (Exception e) {
            logger.error("Error during timestamp processing: {}", e.getMessage(), e);
            return PipelineStepResult.failure("Timestamp processing error: " + e.getMessage(), e);
        }
    }
    
    /**
     * Parse timestamp in various formats (array or string)
     */
    private LocalDateTime parseTimestamp(Object timestampObj) {
        if (timestampObj == null) {
            return null;
        }
        
        try {
            if (timestampObj instanceof List<?> timeArray) {
                // Handle array format: [2025, 6, 25, 10, 7, 25, 754055364]
                @SuppressWarnings("unchecked")
                List<Integer> timeList = (List<Integer>) timeArray;
                if (timeList.size() >= 6) {
                    return LocalDateTime.of(
                        timeList.get(0), // year
                        timeList.get(1), // month
                        timeList.get(2), // day
                        timeList.get(3), // hour
                        timeList.get(4), // minute
                        timeList.get(5)  // second
                        // Note: nanoseconds (7th element) are ignored for simplicity
                    );
                }
            } else if (timestampObj instanceof String) {
                // Handle string format
                return LocalDateTime.parse((String) timestampObj);
            }
        } catch (Exception e) {
            logger.warn("Failed to parse timestamp object {}: {}", timestampObj, e.getMessage());
        }
        
        return null;
    }
    
    @Override
    public String getStepName() {
        return "TimestampProcessing";
    }
    
    @Override
    public boolean continueOnFailure() {
        return true; // Continue even if timestamp processing fails
    }
    
    @Override
    public int getExecutionOrder() {
        return 30; // Execute after message conversion
    }
}
