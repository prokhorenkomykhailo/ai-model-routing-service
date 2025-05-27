package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.client.AuthServiceClient;
import com.lucid.automation.airouting.dto.TenantDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import feign.FeignException;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.Optional;

/**
 * Service for tenant-related operations that uses the AuthServiceClient to fetch tenant data
 */
@Service
public class TenantService {
    
    private static final Logger logger = LoggerFactory.getLogger(TenantService.class);
    
    private final AuthServiceClient authServiceClient;
    
    @Autowired
    public TenantService(AuthServiceClient authServiceClient) {
        this.authServiceClient = authServiceClient;
    }
    
    /**
     * Get all tenants from the auth-service
     * 
     * @return list of all tenants, or empty list if an error occurs
     */
    public List<TenantDTO> getAllTenants() {
        try {
            logger.info("Fetching all tenants from auth-service");
            return authServiceClient.getAllTenants();
        } catch (FeignException e) {
            logger.error("Error fetching tenants from auth-service: {}", e.getMessage(), e);
            return Collections.emptyList();
        }
    }
    
    /**
     * Get a tenant by its UUID
     * 
     * @param tenantId the UUID of the tenant to retrieve
     * @return an Optional containing the tenant if found, or empty if not found or an error occurs
     */
    public Optional<TenantDTO> getTenantById(UUID tenantId) {
        try {
            logger.info("Fetching tenant with ID {} from auth-service", tenantId);
            return Optional.of(authServiceClient.getTenantById(tenantId));
        } catch (FeignException.NotFound e) {
            logger.warn("Tenant with ID {} not found", tenantId);
            return Optional.empty();
        } catch (FeignException e) {
            logger.error("Error fetching tenant with ID {} from auth-service: {}", tenantId, e.getMessage(), e);
            return Optional.empty();
        }
    }
}
