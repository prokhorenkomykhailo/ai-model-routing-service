package com.lucid.automation.airouting.repository;

import com.lucid.automation.airouting.model.EnrichmentJob;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface EnrichmentJobRepository extends CrudRepository<EnrichmentJob, String> {
    Iterable<EnrichmentJob> findByUserIdOrderByCreatedAtDesc(String userId);
    Iterable<EnrichmentJob> findByUserIdAndTenantIdOrderByCreatedAtDesc(String userId, String tenantId);
    Iterable<EnrichmentJob> findByParentId(String parentId);
}