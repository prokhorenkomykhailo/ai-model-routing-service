package com.lucid.automation.airouting.pipeline.ingestion;

import com.lucid.automation.common.dto.messaging.IngestionEventDTO;
import com.lucid.automation.airouting.model.Message;
import com.lucid.automation.airouting.model.User;
import com.lucid.automation.airouting.model.Workspace;

import java.util.HashMap;
import java.util.Map;

/**
 * Processing context specific to the ingestion pipeline.
 * Contains the original ingestion event and accumulated results from processors.
 * 
 * @author AI Assistant
 */
public class IngestionProcessingContext {
    
    private final IngestionEventDTO ingestionEvent;
    private final Map<String, Object> processingData;
    
    // Processed entities specific to ingestion pipeline
    private Message processedMessage;
    private User processedUser;
    private Workspace processedWorkspace;
    
    // Ingestion-specific flags and metadata
    private boolean messageStored = false;
    private boolean userProcessed = false;
    private boolean workspaceProcessed = false;
    private String messageId;
    private String tenantId;
    
    public IngestionProcessingContext(IngestionEventDTO ingestionEvent) {
        this.ingestionEvent = ingestionEvent;
        this.processingData = new HashMap<>();
        this.messageId = ingestionEvent.getMessage() != null ? ingestionEvent.getMessage().getTs() : null;
        this.tenantId = ingestionEvent.getTenantId();
    }
    
    /**
     * Gets the original ingestion event.
     * 
     * @return The ingestion event DTO
     */
    public IngestionEventDTO getIngestionEvent() {
        return ingestionEvent;
    }
    
    /**
     * Gets the message ID for this processing context.
     * 
     * @return The message ID
     */
    public String getMessageId() {
        return messageId;
    }
    
    /**
     * Gets the tenant ID for this processing context.
     * 
     * @return The tenant ID
     */
    public String getTenantId() {
        return tenantId;
    }
    
    /**
     * Stores arbitrary processing data that can be shared between processors.
     * 
     * @param key The data key
     * @param value The data value
     */
    public void setProcessingData(String key, Object value) {
        processingData.put(key, value);
    }
    
    /**
     * Retrieves processing data by key.
     * 
     * @param key The data key
     * @return The data value, or null if not found
     */
    public Object getProcessingData(String key) {
        return processingData.get(key);
    }
    
    /**
     * Retrieves processing data by key with type casting.
     * 
     * @param key The data key
     * @param type The expected type
     * @param <T> The type parameter
     * @return The typed data value, or null if not found or wrong type
     */
    @SuppressWarnings("unchecked")
    public <T> T getProcessingData(String key, Class<T> type) {
        Object value = processingData.get(key);
        if (value != null && type.isInstance(value)) {
            return (T) value;
        }
        return null;
    }
    
    /**
     * Checks if processing data exists for the given key.
     * 
     * @param key The data key
     * @return true if data exists, false otherwise
     */
    public boolean hasProcessingData(String key) {
        return processingData.containsKey(key);
    }
    
    // Getters and setters for processed entities
    
    public Message getProcessedMessage() {
        return processedMessage;
    }
    
    public void setProcessedMessage(Message processedMessage) {
        this.processedMessage = processedMessage;
    }
    
    public User getProcessedUser() {
        return processedUser;
    }
    
    public void setProcessedUser(User processedUser) {
        this.processedUser = processedUser;
    }
    
    public Workspace getProcessedWorkspace() {
        return processedWorkspace;
    }
    
    public void setProcessedWorkspace(Workspace processedWorkspace) {
        this.processedWorkspace = processedWorkspace;
    }
    
    // Ingestion-specific status flags
    
    public boolean isMessageStored() {
        return messageStored;
    }
    
    public void setMessageStored(boolean messageStored) {
        this.messageStored = messageStored;
    }
    
    public boolean isUserProcessed() {
        return userProcessed;
    }
    
    public void setUserProcessed(boolean userProcessed) {
        this.userProcessed = userProcessed;
    }
    
    public boolean isWorkspaceProcessed() {
        return workspaceProcessed;
    }
    
    public void setWorkspaceProcessed(boolean workspaceProcessed) {
        this.workspaceProcessed = workspaceProcessed;
    }
    
    @Override
    public String toString() {
        return String.format("IngestionProcessingContext{messageId='%s', tenantId='%s', " +
                           "messageStored=%s, userProcessed=%s, workspaceProcessed=%s, " +
                           "processedEntities=[message=%s, user=%s, workspace=%s]}", 
                           messageId, tenantId,
                           messageStored, userProcessed, workspaceProcessed,
                           processedMessage != null ? "✓" : "✗",
                           processedUser != null ? "✓" : "✗",
                           processedWorkspace != null ? "✓" : "✗");
    }
}
