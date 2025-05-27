package com.lucid.automation.airouting.controller;

import com.lucid.automation.airouting.dto.MessageWithContentDTO;
import com.lucid.automation.airouting.service.DataStorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * Controller for data-storage-service related operations
 * This controller provides endpoints to interact with the data-storage-service via Feign clients
 */
@RestController
@RequestMapping("/api/data-storage")
public class DataStorageController {
    
    private static final Logger logger = LoggerFactory.getLogger(DataStorageController.class);
    
    private final DataStorageService dataStorageService;
    
    @Autowired
    public DataStorageController(DataStorageService dataStorageService) {
        this.dataStorageService = dataStorageService;
    }
    
    /**
     * Get all distinct group IDs
     * 
     * @param tenantId the tenant ID for multi-tenancy support
     * @param tenantSchema the tenant schema for multi-tenancy support
     * @return list of all group IDs
     */
    @GetMapping("/group-ids")
    public ResponseEntity<List<String>> getAllGroupIds(
            @RequestHeader("X-Tenant-ID") String tenantId,
            @RequestHeader("X-Tenant-Schema") String tenantSchema) {
        logger.info("Received request to get all group IDs for tenant: {}", tenantId);
        List<String> groupIds = dataStorageService.getAllGroupIds(tenantId, tenantSchema);
        return ResponseEntity.ok(groupIds);
    }
    
    /**
     * Get all messages by group ID with content
     * 
     * @param groupId the group ID to retrieve messages for
     * @param tenantId the tenant ID for multi-tenancy support
     * @param tenantSchema the tenant schema for multi-tenancy support
     * @return list of messages with content for the specified group
     */
    @GetMapping("/messages/group/{groupId}")
    public ResponseEntity<List<MessageWithContentDTO>> getAllMessagesByGroupId(
            @PathVariable String groupId,
            @RequestHeader("X-Tenant-ID") String tenantId,
            @RequestHeader("X-Tenant-Schema") String tenantSchema) {
        logger.info("Received request to get messages for group ID: {} for tenant: {}", groupId, tenantId);
        List<MessageWithContentDTO> messages = dataStorageService.getAllMessagesByGroupId(groupId, tenantId, tenantSchema);
        return ResponseEntity.ok(messages);
    }
}
