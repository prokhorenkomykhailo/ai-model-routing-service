package com.lucid.automation.airouting.client;

import com.lucid.automation.common.dto.response.APIResponse;
import com.lucid.automation.airouting.dto.CategoryDTO;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;

import java.util.List;

/**
 * Feign client for the data-storage-service endpoints
 */
@FeignClient(name = "data-storage-service", configuration = FeignClientConfig.class)
public interface DataStorageServiceClient {
    
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
