package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.model.EnrichmentJob;
import com.lucid.automation.airouting.repository.EnrichmentJobRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
     * Returns null if no job is found, allowing callers to handle the missing job appropriately.
     *
     * @param userId The user ID to search for
     * @param tenantId The tenant ID to search for
     * @return The latest EnrichmentJob, or null if none exists
     */
    public EnrichmentJob getLatestJobByUserIdAndTenantId(String userId, String tenantId) {
        Iterable<EnrichmentJob> jobs = enrichmentJobRepository.findByUserIdAndTenantIdOrderByCreatedAtDesc(userId, tenantId);
        if (jobs.iterator().hasNext()) {
            return jobs.iterator().next();
        }
        logger.debug("No enrichment job found for userId={}, tenantId={}", userId, tenantId);
        return null;
    }
}