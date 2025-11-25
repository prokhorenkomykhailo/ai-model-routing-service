package com.lucid.automation.airouting.controller;

import com.lucid.automation.airouting.audit.Audit;
import com.lucid.automation.airouting.model.EnrichmentJob;
import com.lucid.automation.airouting.service.EnrichmentJobService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

/**
 * REST Controller for managing enrichment jobs
 */
@RestController
@RequestMapping("/jobs")
@CrossOrigin(origins = "*")
@Tag(name = "Job Management", description = "API for managing enrichment jobs")
public class JobController {

    private static final Logger logger = LoggerFactory.getLogger(JobController.class);

    private final EnrichmentJobService enrichmentJobService;

    public JobController(EnrichmentJobService enrichmentJobService) {
        this.enrichmentJobService = enrichmentJobService;
    }

    /**
     * Get all enrichment jobs for a specific user and tenant
     */
    @GetMapping
    @Audit(action = "AI_JOBS_GET", description = "User retrieved enrichment jobs")
    @Operation(summary = "Get all enrichment jobs for a user", description = "Retrieves all enrichment jobs for a specific user and tenant. Requires X-User-Id, X-Tenant-Id, and X-Tenant-Schema headers.")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Jobs retrieved successfully")
    })
    public ResponseEntity<List<EnrichmentJob>> getAllJobs(
            @RequestHeader("X-User-Id") String userId,
            @RequestHeader("X-Tenant-Id") String tenantId,
            @RequestHeader("X-Tenant-Schema") String tenantSchema) {
        logger.debug("Getting all enrichment jobs for userId={}, tenantId={}, tenantSchema={}", userId, tenantId, tenantSchema);
        List<EnrichmentJob> jobs = new ArrayList<>();
        enrichmentJobService.getAllJobs(userId).forEach(jobs::add); // Filtering by userId only for now
        logger.info("Retrieved {} enrichment jobs for userId={}, tenantId={}, tenantSchema={}", jobs.size(), userId, tenantId, tenantSchema);
        return ResponseEntity.ok(jobs);
    }

    /**
     * Get the latest enrichment job for a user in a tenant/schema
     */
    @GetMapping("/latest")
    @Audit(action = "AI_JOB_LATEST_GET", description = "User retrieved latest enrichment job")
    @Operation(summary = "Get latest enrichment job by user", description = "Retrieves the latest enrichment job for a user. Requires X-User-Id, X-Tenant-Id, and X-Tenant-Schema headers.")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Latest job retrieved successfully"),
        @ApiResponse(responseCode = "404", description = "No job found for user/tenant/schema")
    })
    public ResponseEntity<EnrichmentJob> getLatestJobByUser(
            @RequestHeader("X-User-Id") String userId,
            @RequestHeader("X-Tenant-Id") String tenantId,
            @RequestHeader("X-Tenant-Schema") String tenantSchema) {
        logger.debug("Getting latest enrichment job for userId={}, tenantId={}, tenantSchema={}", userId, tenantId, tenantSchema);
        EnrichmentJob job = enrichmentJobService.getLatestJobByUserIdAndTenantId(userId, tenantId);
        if (job != null) {
            return ResponseEntity.ok(job);
        } else {
            logger.info("No enrichment job found for userId={}, tenantId={}, tenantSchema={}", userId, tenantId, tenantSchema);
            return ResponseEntity.notFound().build();
        }
    }
}
