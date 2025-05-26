package com.lucid.automation.airouting.model.request;

import jakarta.validation.constraints.NotBlank;

public class CategoryRequest {
    @NotBlank
    private String content;
    private String tenantId;
    private String preferredProvider;
    
    // Constructors
    public CategoryRequest() {}
    
    public CategoryRequest(String content) {
        this.content = content;
    }
    
    // Getters and Setters
    public String getContent() { return content; }
    public void setContent(String content) { this.content = content; }
    
    public String getTenantId() { return tenantId; }
    public void setTenantId(String tenantId) { this.tenantId = tenantId; }
    
    public String getPreferredProvider() { return preferredProvider; }
    public void setPreferredProvider(String preferredProvider) { this.preferredProvider = preferredProvider; }
}
