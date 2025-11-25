package com.lucid.automation.airouting.pipeline.postprocessing;

import com.lucid.automation.airouting.pipeline.postprocessing.step.*;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Factory for creating different pipeline configurations for post-processing
 */
@Component
public class PostProcessingPipelineFactory {
    
    private final InputValidationStep inputValidationStep;
    private final MessageConversionStep messageConversionStep;
    private final TimestampProcessingStep timestampProcessingStep;
    private final ConversationEnrichmentStep conversationEnrichmentStep;
    private final TopicEnrichmentStep topicEnrichmentStep;
    private final ResponseBuildingStep responseBuildingStep;
    
    public PostProcessingPipelineFactory(
            InputValidationStep inputValidationStep,
            MessageConversionStep messageConversionStep,
            TimestampProcessingStep timestampProcessingStep,
            ConversationEnrichmentStep conversationEnrichmentStep,
            TopicEnrichmentStep topicEnrichmentStep,
            ResponseBuildingStep responseBuildingStep) {
        this.inputValidationStep = inputValidationStep;
        this.messageConversionStep = messageConversionStep;
        this.timestampProcessingStep = timestampProcessingStep;
        this.conversationEnrichmentStep = conversationEnrichmentStep;
        this.topicEnrichmentStep = topicEnrichmentStep;
        this.responseBuildingStep = responseBuildingStep;
    }
    
    /**
     * Create the standard post-processing pipeline
     */
    public List<PipelineStep> createStandardPipeline() {
        return List.of(
            inputValidationStep,
            messageConversionStep,
            timestampProcessingStep,
            conversationEnrichmentStep,
            topicEnrichmentStep,
            responseBuildingStep
        );
    }
    
    /**
     * Create a comprehensive pipeline with all enrichment steps
     */
    public List<PipelineStep> createComprehensivePipeline() {
        return List.of(
            inputValidationStep,
            messageConversionStep,
            timestampProcessingStep,
            conversationEnrichmentStep,
            topicEnrichmentStep,
            responseBuildingStep
        );
    }
    
    /**
     * Create a minimal pipeline for basic processing
     */
    public List<PipelineStep> createMinimalPipeline() {
        return List.of(
            inputValidationStep,
            responseBuildingStep
        );
    }
    
    /**
     * Create a validation-only pipeline for testing
     */
    public List<PipelineStep> createValidationOnlyPipeline() {
        return List.of(
            inputValidationStep
        );
    }
    
    /**
     * Create a custom pipeline with specific steps
     */
    public List<PipelineStep> createCustomPipeline(List<PipelineStepType> stepTypes) {
        return stepTypes.stream()
            .map(this::getStepByType)
            .toList();
    }
    
    private PipelineStep getStepByType(PipelineStepType stepType) {
        return switch (stepType) {
            case INPUT_VALIDATION -> inputValidationStep;
            case MESSAGE_CONVERSION -> messageConversionStep;
            case TIMESTAMP_PROCESSING -> timestampProcessingStep;
            case CONVERSATION_ENRICHMENT -> conversationEnrichmentStep;
            case TOPIC_ENRICHMENT -> topicEnrichmentStep;
            case RESPONSE_BUILDING -> responseBuildingStep;
        };
    }
    
    /**
     * Enum for pipeline step types
     */
    public enum PipelineStepType {
        INPUT_VALIDATION,
        MESSAGE_CONVERSION,
        TIMESTAMP_PROCESSING,
        CONVERSATION_ENRICHMENT,
        TOPIC_ENRICHMENT,
        RESPONSE_BUILDING
    }
}
