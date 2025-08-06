package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.model.EnrichmentJob;
import com.lucid.automation.airouting.repository.EnrichmentJobRepository;

import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cglib.core.Local;
import org.springframework.stereotype.Service;

@Service
public class EnrichmentJobService {

    private static final Logger logger = LoggerFactory.getLogger(EnrichmentJobService.class);

    private final EnrichmentJobRepository enrichmentJobRepository;

    public EnrichmentJobService(EnrichmentJobRepository enrichmentJobRepository) {
        this.enrichmentJobRepository = enrichmentJobRepository;
    }

    /**
     * Create and persist a new EnrichmentJob.
     */
    public EnrichmentJob create(EnrichmentJob job) {
        return enrichmentJobRepository.save(job);
    }

    /**
     * Retrieve an EnrichmentJob by its id.
     */
    public EnrichmentJob getById(String id) {
        return enrichmentJobRepository.findById(id).orElse(null);
    }
    /**
     * Retrieve all EnrichmentJobs for a specific userId.
     */
    public Iterable<EnrichmentJob> getAllJobs(String userId) {
        return enrichmentJobRepository.findByUserIdOrderByCreatedAtDesc(userId);
    }

    /**
     * Retrieve the latest EnrichmentJob for a user by userId only.
     */
    public EnrichmentJob getLatestJobByUserId(String userId) {
        Iterable<EnrichmentJob> jobs = enrichmentJobRepository.findByUserIdOrderByCreatedAtDesc(userId);
        if (jobs.iterator().hasNext()) {
            return jobs.iterator().next();
        }
        return null;
    }

    /**
     * Retrieve the latest EnrichmentJob for a user by userId and tenantId.
     */
    public EnrichmentJob getLatestJobByUserIdAndTenantId(String userId, String tenantId) {
        Iterable<EnrichmentJob> jobs = enrichmentJobRepository.findByUserIdAndTenantIdOrderByCreatedAtDesc(userId, tenantId);
        if (jobs.iterator().hasNext()) {
            return jobs.iterator().next();
        }
        // if no job found, return a default job specifying that job is not completed
        EnrichmentJob defaultJob = new EnrichmentJob();
        defaultJob.setUserId(userId);
        defaultJob.setTenantId(tenantId);
        // set other default values as needed
        defaultJob.setStartTime(null);
        defaultJob.setEndTime(null);
        defaultJob.setStatus("PENDING");
        defaultJob.setEstimatedCompletionTime(LocalDateTime.now().plusMinutes(10).toString()); // Example: 30 minutes from now
        defaultJob.setCreatedAt(System.currentTimeMillis());
        defaultJob.setUpdatedAt(System.currentTimeMillis());
        defaultJob.setProgress(0.0);
        defaultJob.setDurationMs(0L);
        defaultJob.setEstimatedTimeLeft(10*60L);
        defaultJob.setType("ENRICHMENT");
        defaultJob.setResult(null);
        defaultJob.setTenantSchema(null); // Set tenant schema if needed
        defaultJob.setId("default-" + userId + "-" + tenantId + "-" + System.currentTimeMillis()); // Generate a unique ID
        defaultJob.setParentId(null); // No parent job for default
        return defaultJob;
    }
}