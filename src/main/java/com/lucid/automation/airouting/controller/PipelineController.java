package com.lucid.automation.airouting.controller;

import com.lucid.automation.airouting.pipeline.config.PipelineConfiguration;
import com.lucid.automation.airouting.pipeline.ingestion.IngestionPipelineOrchestrator;
import com.lucid.automation.airouting.pipeline.postprocessing.PostProcessingPipelineOrchestrator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * REST controller for pipeline status and health checks
 * Updated to support the new separated pipeline architecture
 */
@RestController
@RequestMapping("/api/pipeline")
public class PipelineController {

    @Autowired
    private PipelineConfiguration pipelineConfiguration;
    
    @Autowired
    private IngestionPipelineOrchestrator ingestionOrchestrator;
    
    @Autowired
    private PostProcessingPipelineOrchestrator postProcessingOrchestrator;

    /**
     * Get pipeline configuration status
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getPipelineStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("enabled", pipelineConfiguration.isEnabled());
        status.put("architecture", "separated"); // ingestion + postprocessing pipelines
        status.put("pipelines", Map.of(
            "ingestion", "com.lucid.automation.airouting.pipeline.ingestion",
            "postprocessing", "com.lucid.automation.airouting.pipeline.postprocessing"
        ));
        status.put("continueOnFailure", pipelineConfiguration.isContinueOnFailure());
        status.put("maxExecutionTimeMs", pipelineConfiguration.getMaxExecutionTimeMs());
        status.put("detailedLogging", pipelineConfiguration.isDetailedLogging());
        status.put("metricsEnabled", pipelineConfiguration.isMetricsEnabled());
        
        return ResponseEntity.ok(status);
    }

    /**
     * Get pipeline health check
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> getPipelineHealth() {
        Map<String, Object> health = new HashMap<>();
        
        if (pipelineConfiguration.isEnabled()) {
            health.put("status", "UP");
            health.put("message", "Pipeline is enabled and ready");
        } else {
            health.put("status", "DOWN");
            health.put("message", "Pipeline is disabled");
        }
        
        health.put("timestamp", System.currentTimeMillis());
        
        return ResponseEntity.ok(health);
    }

    /**
     * Get detailed information about ingestion pipeline
     */
    @GetMapping("/ingestion/info")
    public ResponseEntity<Map<String, Object>> getIngestionPipelineInfo() {
        Map<String, Object> info = new HashMap<>();
        info.put("type", "ingestion");
        info.put("package", "com.lucid.automation.airouting.pipeline.ingestion");
        info.put("orchestrator", ingestionOrchestrator.getClass().getSimpleName());
        info.put("processors", Map.of(
            "validation", "ValidationProcessor (order: 10)",
            "messageStorage", "MessageStorageProcessor (order: 20)", 
            "userProcessing", "UserProcessingProcessor (order: 30)",
            "workspaceProcessing", "WorkspaceProcessingProcessor (order: 40)"
        ));
        info.put("description", "Processes incoming Kafka messages through modular processors");
        
        return ResponseEntity.ok(info);
    }

    /**
     * Get detailed information about post-processing pipeline
     */
    @GetMapping("/postprocessing/info")
    public ResponseEntity<Map<String, Object>> getPostProcessingPipelineInfo() {
        Map<String, Object> info = new HashMap<>();
        info.put("type", "postprocessing");
        info.put("package", "com.lucid.automation.airouting.pipeline.postprocessing");
        info.put("orchestrator", postProcessingOrchestrator.getClass().getSimpleName());
        info.put("steps", Map.of(
            "inputValidation", "InputValidationStep (order: 10)",
            "messageConversion", "MessageConversionStep (order: 20)",
            "timestampProcessing", "TimestampProcessingStep (order: 30)",
            "conversationEnrichment", "ConversationEnrichmentStep (order: 40)",
            "topicEnrichment", "TopicEnrichmentStep (order: 50)",
            "responseBuilding", "ResponseBuildingStep (order: 90)"
        ));
        info.put("description", "Processes AI responses through enrichment steps");
        
        return ResponseEntity.ok(info);
    }

    /**
     * Get overview of both pipelines
     */
    @GetMapping("/overview")
    public ResponseEntity<Map<String, Object>> getPipelineOverview() {
        Map<String, Object> overview = new HashMap<>();
        overview.put("architecture", "Separated Pipeline Architecture");
        overview.put("totalPipelines", 2);
        overview.put("pipelines", Map.of(
            "ingestion", Map.of(
                "purpose", "Process incoming Kafka messages",
                "processors", 4,
                "endpoint", "/api/pipeline/ingestion/info"
            ),
            "postprocessing", Map.of(
                "purpose", "Process AI responses with enrichment",
                "steps", 6,
                "endpoint", "/api/pipeline/postprocessing/info"
            )
        ));
        overview.put("configuration", Map.of(
            "enabled", pipelineConfiguration.isEnabled(),
            "continueOnFailure", pipelineConfiguration.isContinueOnFailure(),
            "maxExecutionTimeMs", pipelineConfiguration.getMaxExecutionTimeMs(),
            "detailedLogging", pipelineConfiguration.isDetailedLogging(),
            "metricsEnabled", pipelineConfiguration.isMetricsEnabled()
        ));
        
        return ResponseEntity.ok(overview);
    }
}
