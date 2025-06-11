package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.client.AuthServiceClient;
import com.lucid.automation.airouting.dto.TenantDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import feign.FeignException;

import java.util.Collections;
import java.util.List;

/**
 * Service for tenant-related operations that uses the AuthServiceClient to fetch tenant data
 */
@Service
public class TenantService {
    
    private static final Logger logger = LoggerFactory.getLogger(TenantService.class);
    
    private final AuthServiceClient authServiceClient;
    
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
}
