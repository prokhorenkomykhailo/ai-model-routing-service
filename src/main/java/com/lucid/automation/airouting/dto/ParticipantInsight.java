package com.lucid.automation.airouting.dto;

public record ParticipantInsight(
    double engagementLevel,
    String dominantSentiment,
    int messageCount
) {}