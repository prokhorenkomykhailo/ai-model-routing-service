package com.lucid.automation.airouting.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * DTO representing user information
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public record UserDTO(
    @JsonProperty("id")
    String id,
    
    @JsonProperty("username")
    String username,
    
    @JsonProperty("displayName")
    String displayName,
    
    @JsonProperty("imageUrl")
    String imageUrl
) {
    
    /**
     * Create UserDTO from just a user ID/name (for backward compatibility)
     */
    public static UserDTO fromString(String userIdOrName) {
        if (userIdOrName == null || userIdOrName.trim().isEmpty()) {
            return new UserDTO(null, null, null, null);
        }
        
        String trimmed = userIdOrName.trim();
        return new UserDTO(
            trimmed,
            trimmed,
            trimmed,
            trimmed.length() > 0 ? 
                "https://via.placeholder.com/40x40?text=" + trimmed.charAt(0) : 
                "https://via.placeholder.com/40x40?text=U"
        );
    }
}
