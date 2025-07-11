package com.lucid.automation.airouting.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * DTO for message responses (without internal composite indexes)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MessageResponseDTO {
    
    private String id;
    private String tenantId;
    private String workspaceId;
    private String channelId;
    private String threadTs;
    private String messageTs;
    private String userId;
    private String deemergeUserId; // Deemerge user ID
    private String channelName;    // Slack channel name
    private String username;
    private String text;
    private String messageType;
    private String subtype;
    private Map<String, Object> metadata;
    private Long ingestedAt;
}
