package com.lucid.automation.airouting.model.response;

import java.time.LocalDateTime;

public class HealthStatus {
    private String status;
    private LocalDateTime timestamp;
    private String version;
    private String serviceId;
    
    // Constructors
    public HealthStatus() {
        this.timestamp = LocalDateTime.now();
        this.serviceId = "lucid-ai-routing-service";
        this.version = "1.0.0";
    }
    
    // Getters and Setters
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    
    public LocalDateTime getTimestamp() { return timestamp; }
    public void setTimestamp(LocalDateTime timestamp) { this.timestamp = timestamp; }
    
    public String getVersion() { return version; }
    public void setVersion(String version) { this.version = version; }
    
    public String getServiceId() { return serviceId; }
    public void setServiceId(String serviceId) { this.serviceId = serviceId; }
}
