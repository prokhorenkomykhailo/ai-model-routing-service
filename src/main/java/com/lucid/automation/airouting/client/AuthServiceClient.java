package com.lucid.automation.airouting.client;

import com.lucid.automation.airouting.dto.TenantDTO;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import java.util.List;
import java.util.UUID;

/**
 * Feign client for the auth-service's tenant endpoints
 */
@FeignClient(name = "auth-service", path = "/tenants", configuration = FeignClientConfig.class)
public interface AuthServiceClient {
    
    /**
     * Get all tenants from the auth-service
     * 
     * @return list of all tenants
     */
    @GetMapping
    List<TenantDTO> getAllTenants();
    
    /**
     * Get a tenant by its UUID
     * 
     * @param tenantId the UUID of the tenant to retrieve
     * @return the tenant information
     */
    @GetMapping("/{tenantId}")
    TenantDTO getTenantById(@PathVariable UUID tenantId);
}
