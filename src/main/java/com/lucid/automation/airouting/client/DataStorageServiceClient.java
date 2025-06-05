package com.lucid.automation.airouting.client;

import com.lucid.automation.airouting.dto.APIResponse;
import com.lucid.automation.airouting.dto.CategoryDTO;
import com.lucid.automation.airouting.dto.EnrichedMessageDTO;
import com.lucid.automation.airouting.dto.MessageWithContentDTO;
import com.lucid.automation.airouting.dto.StoredDataDTO;
import com.lucid.automation.airouting.dto.TopicDTO;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;
import java.util.Map;

/**
 * Feign client for the data-storage-service endpoints
 */
@FeignClient(name = "data-storage-service", configuration = FeignClientConfig.class)
public interface DataStorageServiceClient {
    
    /**
     * Get all distinct group IDs from messages
     * 
     * @param tenantId the tenant ID for multi-tenancy support
     * @param tenantSchema the tenant schema for multi-tenancy support
     * @return APIResponse containing list of all group IDs
     */
    @GetMapping("/messages/group-ids")
    APIResponse<List<String>> getAllGroupIds(
            @RequestHeader("X-Tenant-Id") String tenantId,
            @RequestHeader("X-Tenant-Schema") String tenantSchema
    );
    
    /**
     * Get all messages by group ID with their content
     * 
     * @param groupId the group ID to retrieve messages for
     * @param tenantId the tenant ID for multi-tenancy support
     * @param tenantSchema the tenant schema for multi-tenancy support
     * @return APIResponse containing list of messages with content for the specified group
     */
    @GetMapping("/messages/group/{groupId}")
    APIResponse<List<MessageWithContentDTO>> getAllMessagesByGroupId(
            @PathVariable("groupId") String groupId,
            @RequestHeader("X-Tenant-Id") String tenantId,
            @RequestHeader("X-Tenant-Schema") String tenantSchema
    );
    
    /**
     * Save enriched message data
     * 
     * @param enrichedMessage the enriched message to save
     * @param tenantId the tenant ID for multi-tenancy support
     * @param tenantSchema the tenant schema for multi-tenancy support
     * @param metadata optional additional metadata
     * @return APIResponse containing the stored data
     */
    @PostMapping("/data/enriched-messages")
    APIResponse<StoredDataDTO> saveEnrichedMessage(
            @RequestBody EnrichedMessageDTO enrichedMessage,
            @RequestHeader("X-Tenant-Id") String tenantId,
            @RequestHeader("X-Tenant-Schema") String tenantSchema,
            @RequestParam(required = false) Map<String, Object> metadata
    );
    
    /**
     * Create a new topic
     * 
     * @param topicDTO the topic data to create
     * @param tenantId the tenant ID for multi-tenancy support
     * @param tenantSchema the tenant schema for multi-tenancy support
     * @return APIResponse containing the created topic
     */
    @PostMapping("/topics")
    APIResponse<TopicDTO> createTopic(
            @RequestBody TopicDTO topicDTO,
            @RequestHeader("X-Tenant-Id") String tenantId,
            @RequestHeader("X-Tenant-Schema") String tenantSchema
    );
    
    /**
     * Get all categories
     * 
     * @param tenantId the tenant ID for multi-tenancy support
     * @param tenantSchema the tenant schema for multi-tenancy support
     * @return APIResponse containing list of all categories
     */
    @GetMapping("/categories")
    APIResponse<List<CategoryDTO>> getAllCategories(
            @RequestHeader("X-Tenant-Id") String tenantId,
            @RequestHeader("X-Tenant-Schema") String tenantSchema
    );
}
