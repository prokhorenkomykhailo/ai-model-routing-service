package com.lucid.automation.airouting.pipeline.context;

import com.lucid.automation.common.dto.messaging.IngestionEventDTO;
import com.lucid.automation.airouting.model.Message;
import com.lucid.automation.airouting.model.User;
import com.lucid.automation.airouting.model.Workspace;

import java.util.HashMap;
import java.util.Map;

/**
 * Processing context that carries data through the message processing pipeline.
 * Contains the original ingestion event and accumulated results from processors.
 * 
 * @author AI Assistant
 */
public class MessageProcessingContext {
    
    private final IngestionEventDTO ingestionEvent;
    private final Map<String, Object> processingData;
    
    // Processed entities
    private Message processedMessage;
    private User processedUser;
    private Workspace processedWorkspace;
    
    public MessageProcessingContext(IngestionEventDTO ingestionEvent) {
        this.ingestionEvent = ingestionEvent;
        this.processingData = new HashMap<>();
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
    
    @Override
    public String toString() {
        return String.format("MessageProcessingContext{tenantId='%s', messageId='%s', processedEntities=[message=%s, user=%s, workspace=%s]}", 
                           ingestionEvent.getTenantId(),
                           ingestionEvent.getMessage() != null ? ingestionEvent.getMessage().getTs() : "null",
                           processedMessage != null ? "✓" : "✗",
                           processedUser != null ? "✓" : "✗",
                           processedWorkspace != null ? "✓" : "✗");
    }
}
