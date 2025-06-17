package com.lucid.automation.airouting.dto;

public enum UrgencyLevel {
    LOW(1), MEDIUM(2), HIGH(3), CRITICAL(4);
    
    private final int priority;
    UrgencyLevel(int priority) { this.priority = priority; }
    public int getPriority() { return priority; }
}