package com.lucid.automation.airouting.model;

public class SlackParticipant {
    private String id;
    private String username;
    private String name;
    private String displayName;
    private String realName;
    private String email;
    private String role;
    private String imageUrl;
    private boolean isBot;
    private boolean active;
    private boolean deleted;
    private String timeZone;
    private int timeZoneOffset;
    
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
    
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    
    public String getDisplayName() { return displayName; }
    public void setDisplayName(String displayName) { this.displayName = displayName; }
    
    public String getRealName() { return realName; }
    public void setRealName(String realName) { this.realName = realName; }
    
    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
    
    public String getRole() { return role; }
    public void setRole(String role) { this.role = role; }
    
    public String getImageUrl() { return imageUrl; }
    public void setImageUrl(String imageUrl) { this.imageUrl = imageUrl; }
    
    public boolean isBot() { return isBot; }
    public void setBot(boolean bot) { isBot = bot; }
    
    public boolean isActive() { return active; }
    public void setActive(boolean active) { this.active = active; }
    
    public boolean isDeleted() { return deleted; }
    public void setDeleted(boolean deleted) { this.deleted = deleted; }
    
    public String getTimeZone() { return timeZone; }
    public void setTimeZone(String timeZone) { this.timeZone = timeZone; }
    
    public int getTimeZoneOffset() { return timeZoneOffset; }
    public void setTimeZoneOffset(int timeZoneOffset) { this.timeZoneOffset = timeZoneOffset; }
}
