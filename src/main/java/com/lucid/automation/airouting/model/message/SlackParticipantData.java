package com.lucid.automation.airouting.model.message;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Simplified participant data for RabbitMQ processing
 */
public class SlackParticipantData {
    
    @JsonProperty("id")
    private String id;
    
    @JsonProperty("name")
    private String name;
    
    @JsonProperty("email")
    private String email;
    
    @JsonProperty("role")
    private String role;

    @JsonProperty("imageUrl")
    private String imageUrl;
    
    // Constructors
    public SlackParticipantData() {}
    
    public SlackParticipantData(String id, String name) {
        this.id = id;
        this.name = name;
    }
    
    // Getters and Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    
    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
    
    public String getRole() { return role; }
    public void setRole(String role) { this.role = role; }

    public String getImageUrl() { return imageUrl; }
    public void setImageUrl(String imageUrl) { this.imageUrl = imageUrl; }
    
    @Override
    public String toString() {
        return "SlackParticipantData{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", role='" + role + '\'' +
                ", imageUrl='" + imageUrl + '\'' +
                '}';
    }
}
