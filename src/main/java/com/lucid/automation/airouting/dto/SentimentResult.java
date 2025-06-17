package com.lucid.automation.airouting.dto;

public record SentimentResult(
    String sentiment, 
    double score, 
    double confidence
) {}