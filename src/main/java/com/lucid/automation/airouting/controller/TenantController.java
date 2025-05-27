package com.lucid.automation.airouting.controller;

import com.lucid.automation.airouting.dto.TenantDTO;
import com.lucid.automation.airouting.service.TenantService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

/**
 * Controller for tenant-related operations
 * This is a test controller to verify the Feign client is working
 */
@RestController
@RequestMapping("/api/tenants")
public class TenantController {
    
    private static final Logger logger = LoggerFactory.getLogger(TenantController.class);
    
    private final TenantService tenantService;
    
    @Autowired
    public TenantController(TenantService tenantService) {
        this.tenantService = tenantService;
    }
    
    /**
     * Get all tenants
     * 
     * @return list of all tenants
     */
    @GetMapping
    public ResponseEntity<List<TenantDTO>> getAllTenants() {
        logger.info("Received request to get all tenants");
        List<TenantDTO> tenants = tenantService.getAllTenants();
        return ResponseEntity.ok(tenants);
    }
    
    /**
     * Get a tenant by its UUID
     * 
     * @param tenantId the UUID of the tenant to retrieve
     * @return the tenant information if found
     */
    @GetMapping("/{tenantId}")
    public ResponseEntity<TenantDTO> getTenantById(@PathVariable UUID tenantId) {
        logger.info("Received request to get tenant with ID: {}", tenantId);
        return tenantService.getTenantById(tenantId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
