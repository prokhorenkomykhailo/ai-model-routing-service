package com.lucid.automation.airouting.client;

import com.lucid.automation.airouting.dto.APIResponse;
import com.lucid.automation.airouting.dto.MessageWithContentDTO;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;

import java.util.List;

/**
 * Feign client for the data-storage-service's message endpoints
 */
@FeignClient(name = "data-storage-service", path = "/messages", configuration = FeignClientConfig.class)
public interface DataStorageServiceClient {
    
    /**
     * Get all distinct group IDs from messages
     * 
     * @param tenantId the tenant ID for multi-tenancy support
     * @param tenantSchema the tenant schema for multi-tenancy support
     * @return APIResponse containing list of all group IDs
     */
    @GetMapping("/group-ids")
    APIResponse<List<String>> getAllGroupIds(
            @RequestHeader("X-Tenant-ID") String tenantId,
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
    @GetMapping("/group/{groupId}")
    APIResponse<List<MessageWithContentDTO>> getAllMessagesByGroupId(
            @PathVariable("groupId") String groupId,
            @RequestHeader("X-Tenant-ID") String tenantId,
            @RequestHeader("X-Tenant-Schema") String tenantSchema
    );
}
