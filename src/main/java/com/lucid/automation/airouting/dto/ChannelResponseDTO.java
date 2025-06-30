package com.lucid.automation.airouting.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO for channel response data
 * 
 * @author AI Assistant
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ChannelResponseDTO {
    
    @JsonProperty("channelId")
    private String channelId;
    
    @JsonProperty("channelSrc")
    private String channelSrc;
    
    @JsonProperty("channelName")
    private String channelName;
    
    @JsonProperty("tenantId")
    private String tenantId;
    
    @JsonProperty("workspaceId")
    private String workspaceId;
    
    @JsonProperty("channelType")
    private String channelType;
    
    @JsonProperty("isPrivate")
    private Boolean isPrivate;
    
    @JsonProperty("topic")
    private String topic;
    
    @JsonProperty("purpose")
    private String purpose;
    
    @JsonProperty("createdAt")
    private Long createdAt;
    
    @JsonProperty("updatedAt")
    private Long updatedAt;
    
    @JsonProperty("compositeKey")
    private String compositeKey;
}
