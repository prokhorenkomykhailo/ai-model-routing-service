package com.lucid.automation.airouting.repository;

import com.lucid.automation.airouting.model.Message;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Repository interface for Message Redis operations
 */
@Repository
public interface MessageRepository extends CrudRepository<Message, String> {
    /**
     * Find all messages by tenant ID with pagination
     * 
     * @param tenantId The tenant ID
     * @param pageable Pagination information
     * @return List of messages limited by pagination
     */
    List<Message> findByTenantId(String tenantId, Pageable pageable);

    List<Message> findAllByDeemergeUserId(String deemergeUserId);
       
}
