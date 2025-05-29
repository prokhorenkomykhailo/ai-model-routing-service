package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.client.DataStorageServiceClient;
import com.lucid.automation.airouting.dto.APIResponse;
import com.lucid.automation.airouting.dto.MessageWithContentDTO;
import com.lucid.automation.airouting.dto.TopicDTO;
import feign.FeignException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

/**
 * Service for data-storage-service operations that uses the DataStorageServiceClient to fetch data
 */
@Service
public class DataStorageService {
    
    private static final Logger logger = LoggerFactory.getLogger(DataStorageService.class);
    
    private final DataStorageServiceClient dataStorageServiceClient;
    
    @Autowired
    public DataStorageService(DataStorageServiceClient dataStorageServiceClient) {
        this.dataStorageServiceClient = dataStorageServiceClient;
    }
    
    /**
     * Get all distinct group IDs from the data-storage-service
     * 
     * @param tenantId the tenant ID for multi-tenancy support
     * @param tenantSchema the tenant schema for multi-tenancy support
     * @return list of all group IDs, or empty list if an error occurs
     */
    public List<String> getAllGroupIds(String tenantId, String tenantSchema) {
        try {
            logger.info("Fetching all group IDs from data-storage-service for tenant: {}", tenantId);
            APIResponse<List<String>> response = dataStorageServiceClient.getAllGroupIds(tenantId, tenantSchema);
            
            if (response.isSuccess() && response.getData() != null) {
                logger.info("Successfully fetched {} group IDs", response.getData().size());
                return response.getData();
            } else {
                logger.warn("Failed to fetch group IDs: {}", response.getError());
                return Collections.emptyList();
            }
        } catch (FeignException e) {
            logger.error("Error fetching group IDs from data-storage-service: {}", e.getMessage(), e);
            return Collections.emptyList();
        }
    }
    
    /**
     * Get all messages by group ID with content from the data-storage-service
     * 
     * @param groupId the group ID to retrieve messages for
     * @param tenantId the tenant ID for multi-tenancy support
     * @param tenantSchema the tenant schema for multi-tenancy support
     * @return list of messages with content for the specified group, or empty list if an error occurs
     */
    public List<MessageWithContentDTO> getAllMessagesByGroupId(String groupId, String tenantId, String tenantSchema) {
        try {
            logger.info("Fetching all messages for group ID: {} from data-storage-service for tenant: {}", groupId, tenantId);
            APIResponse<List<MessageWithContentDTO>> response = dataStorageServiceClient.getAllMessagesByGroupId(groupId, tenantId, tenantSchema);
            
            if (response.isSuccess() && response.getData() != null) {
                logger.info("Successfully fetched {} messages for group ID: {}", response.getData().size(), groupId);
                return response.getData();
            } else {
                logger.warn("Failed to fetch messages for group ID {}: {}", groupId, response.getError());
                return Collections.emptyList();
            }
        } catch (FeignException e) {
            logger.error("Error fetching messages for group ID {} from data-storage-service: {}", groupId, e.getMessage(), e);
            return Collections.emptyList();
        }
    }
    
    /**
     * Get all topics from the data-storage-service
     * 
     * @param tenantId the tenant ID for multi-tenancy support
     * @param tenantSchema the tenant schema for multi-tenancy support
     * @return list of all topics, or empty list if an error occurs
     */
    public List<TopicDTO> getAllTopics(String tenantId, String tenantSchema) {
        try {
            logger.info("Fetching all topics from data-storage-service for tenant: {}", tenantId);
            List<TopicDTO> topics = dataStorageServiceClient.getAllTopics(tenantId, tenantSchema);
            
            if (topics != null) {
                logger.info("Successfully fetched {} topics", topics.size());
                return topics;
            } else {
                logger.warn("Received null response when fetching topics");
                return Collections.emptyList();
            }
        } catch (FeignException e) {
            logger.error("Error fetching topics from data-storage-service: {}", e.getMessage(), e);
            return Collections.emptyList();
        }
    }
    
    /**
     * Create a new topic in the data-storage-service
     * 
     * @param topicDTO the topic data to create
     * @param tenantId the tenant ID for multi-tenancy support
     * @param tenantSchema the tenant schema for multi-tenancy support
     * @return the created topic, or null if an error occurs
     */
    public TopicDTO createTopic(TopicDTO topicDTO, String tenantId, String tenantSchema) {
        try {
            logger.info("Creating new topic: {} for tenant: {}", topicDTO.getName(), tenantId);
            APIResponse<TopicDTO> response = dataStorageServiceClient.createTopic(topicDTO, tenantId, tenantSchema);
            
            if (response.isSuccess() && response.getData() != null) {
                logger.info("Successfully created topic with ID: {}", response.getData().getId());
                return response.getData();
            } else {
                logger.warn("Failed to create topic: {}", response.getError());
                return null;
            }
        } catch (FeignException e) {
            logger.error("Error creating topic in data-storage-service: {}", e.getMessage(), e);
            return null;
        }
    }
}
