package com.lucid.automation.airouting.controller;

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
     * Get all enrichment jobs
     */
    @GetMapping
    @Operation(summary = "Get all enrichment jobs", description = "Retrieves all enrichment jobs in the system")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Jobs retrieved successfully")
    })
    public ResponseEntity<List<EnrichmentJob>> getAllJobs() {
        logger.debug("Getting all enrichment jobs");
        List<EnrichmentJob> jobs = new ArrayList<>();
        enrichmentJobService.getAllJobs().forEach(jobs::add);
        logger.info("Retrieved {} enrichment jobs", jobs.size());
        return ResponseEntity.ok(jobs);
    }

    /**
     * Get the latest enrichment job for a user in a tenant/schema
     */
    @GetMapping("/latest")
    @Operation(summary = "Get latest enrichment job by user", description = "Retrieves the latest enrichment job for a user. Requires X-User-Id header. Optionally supports X-Tenant-Id and X-Tenant-Schema.")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Latest job retrieved successfully"),
        @ApiResponse(responseCode = "404", description = "No job found for user/tenant/schema")
    })
    public ResponseEntity<EnrichmentJob> getLatestJobByUser(
            @RequestHeader("X-User-Id") String userId,
            @RequestHeader(value = "X-Tenant-Id", required = false) String tenantId,
            @RequestHeader(value = "X-Tenant-Schema", required = false) String tenantSchema) {
        logger.debug("Getting latest enrichment job for userId={}, tenantId={}, tenantSchema={}", userId, tenantId, tenantSchema);
        EnrichmentJob job = enrichmentJobService.getLatestJobByUserId(userId);
        if (job != null) {
            return ResponseEntity.ok(job);
        } else {
            logger.info("No enrichment job found for userId={}, tenantId={}, tenantSchema={}", userId, tenantId, tenantSchema);
            return ResponseEntity.notFound().build();
        }
    }
}
