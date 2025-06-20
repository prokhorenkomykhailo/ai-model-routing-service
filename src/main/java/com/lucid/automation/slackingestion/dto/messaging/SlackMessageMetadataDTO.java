package com.lucid.automation.slackingestion.dto.messaging;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * DTO representing metadata associated with a Slack message for AI processing
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SlackMessageMetadataDTO {
    @JsonProperty("channelName")
    private String channelName;
    
    @JsonProperty("channelType")
    private String channelType;
    
    @JsonProperty("workspaceName")
    private String workspaceName;
    
    @JsonProperty("isThreadMessage")
    private boolean isThreadMessage;
    
    @JsonProperty("hasAttachments")
    private boolean hasAttachments;
    
    @JsonProperty("mentionedUsers")
    private List<String> mentionedUsers;
    
    @JsonProperty("hasReactions")
    private boolean hasReactions;
    
    @JsonProperty("messageLength")
    private int messageLength;
    
    @JsonProperty("containsUrls")
    private boolean containsUrls;
    
    @JsonProperty("priority")
    private String priority;
    
    @JsonProperty("source")
    private String source;
    
    @JsonProperty("additionalAttributes")
    private Map<String, Object> additionalAttributes;
}
