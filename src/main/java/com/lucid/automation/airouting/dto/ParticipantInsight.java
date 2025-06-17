package com.lucid.automation.airouting.dto;

public record ParticipantInsight(
    String role,
    double engagementLevel,
    String dominantSentiment,
    int messageCount
) {}