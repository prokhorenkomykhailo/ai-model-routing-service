package com.lucid.automation.slackingestion.dto.messaging;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * DTO representing a Slack user for AI processing
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SlackUserDTO {
    @JsonProperty("id")
    private Long id;
    
    @JsonProperty("slackUserId")
    private String slackUserId;
    
    @JsonProperty("teamId")
    private String teamId;
    
    @JsonProperty("name")
    private String name;
    
    @JsonProperty("emailConfirmed")
    private Boolean emailConfirmed;
    
    @JsonProperty("displayName")
    private String displayName;
    
    @JsonProperty("displayNameNormalized")
    private String displayNameNormalized;
    
    @JsonProperty("realNameNormalized")
    private String realNameNormalized;
    
    @JsonProperty("email")
    private String email;
    
    @JsonProperty("title")
    private String title;
    
    @JsonProperty("phone")
    private String phone;
    
    @JsonProperty("firstName")
    private String firstName;
    
    @JsonProperty("lastName")
    private String lastName;
    
    @JsonProperty("pronouns")
    private String pronouns;
    
    @JsonProperty("statusText")
    private String statusText;
    
    @JsonProperty("avatarHash")
    private String avatarHash;
    
    @JsonProperty("imageOriginal")
    private String imageOriginal;
    
    @JsonProperty("image24")
    private String image24;
    
    @JsonProperty("image32")
    private String image32;
    
    @JsonProperty("image48")
    private String image48;
    
    @JsonProperty("image72")
    private String image72;
    
    @JsonProperty("image192")
    private String image192;
    
    @JsonProperty("image512")
    private String image512;
    
    @JsonProperty("image1024")
    private String image1024;
    
    @JsonProperty("teamName")
    private String teamName;
    
    @JsonProperty("slackUpdatedAt")
    private Long slackUpdatedAt;
    
    @JsonProperty("createdAt")
    private LocalDateTime createdAt;
    
    @JsonProperty("updatedAt")
    private LocalDateTime updatedAt;
}
