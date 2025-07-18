package com.lucid.automation.airouting.controller;

import com.lucid.automation.airouting.pipeline.config.PipelineConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * REST controller for pipeline status and health checks
 */
@RestController
@RequestMapping("/api/pipeline")
public class PipelineController {

    @Autowired
    private PipelineConfiguration pipelineConfiguration;

    /**
     * Get pipeline configuration status
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getPipelineStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("enabled", pipelineConfiguration.isEnabled());
        status.put("type", pipelineConfiguration.getType());
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
}
