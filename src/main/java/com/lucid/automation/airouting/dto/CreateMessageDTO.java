package com.lucid.automation.airouting.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import jakarta.validation.constraints.NotBlank;
import java.util.Map;

/**
 * DTO for creating a new message
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CreateMessageDTO {
    
    @NotBlank(message = "Tenant ID is required")
    private String tenantId;
    
    @NotBlank(message = "Workspace ID is required")
    private String workspaceId;
    
    @NotBlank(message = "Channel ID is required")
    private String channelId;
    
    private String threadTs;
    
    @NotBlank(message = "Message timestamp is required")
    private String messageTs;
    
    @NotBlank(message = "User ID is required")
    private String userId;
    
    private String username;
    
    @NotBlank(message = "Message text is required")
    private String text;
    
    private String messageType;
    
    private String subtype;
    
    private Map<String, Object> metadata;
}
