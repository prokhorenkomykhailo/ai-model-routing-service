package com.lucid.automation.airouting.model;

public class SlackParticipant {
    private String id;
    private String username;
    private String displayName;
    private String email;
    private String role;
    private boolean isBot;
    
    // Constructors
    public SlackParticipant() {}
    
    public SlackParticipant(String id, String username, String displayName) {
        this.id = id;
        this.username = username;
        this.displayName = displayName;
    }
    
    // Getters and Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    
    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }
    
    public String getDisplayName() { return displayName; }
    public void setDisplayName(String displayName) { this.displayName = displayName; }
    
    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
    
    public String getRole() { return role; }
    public void setRole(String role) { this.role = role; }
    
    public boolean isBot() { return isBot; }
    public void setBot(boolean bot) { isBot = bot; }
}
