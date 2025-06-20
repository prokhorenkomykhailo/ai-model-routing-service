package com.lucid.automation.slackingestion.dto.messaging;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * DTO representing a Slack message for AI processing
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SlackMessageDTO {
    @JsonProperty("type")
    private String type;
    
    @JsonProperty("subtype")
    private String subtype;
    
    @JsonProperty("user")
    private String user;
    
    @JsonProperty("username")
    private String username;
    
    @JsonProperty("text")
    private String text;
    
    @JsonProperty("ts")
    private String ts;
    
    @JsonProperty("channelId")
    private String channelId;
    
    @JsonProperty("teamId")
    private String teamId;
    
    @JsonProperty("threadTs")
    private String threadTs;
    
    @JsonProperty("conversationGroupId")
    private String conversationGroupId;
    
    @JsonProperty("ingestedAt")
    private Double ingestedAt;
    
    @JsonProperty("topic")
    private String topic;
    
    @JsonProperty("purpose")
    private String purpose;
    
    @JsonProperty("clientMsgId")
    private String clientMsgId;
    
    @JsonProperty("replyCount")
    private Integer replyCount;
    
    @JsonProperty("replyUsers")
    private List<String> replyUsers;
    
    @JsonProperty("replyUsersCount")
    private Integer replyUsersCount;
    
    @JsonProperty("latestReply")
    private String latestReply;
}
