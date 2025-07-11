package com.lucid.automation.airouting.dto;

import java.util.List;

public record MessageEnrichment(
    String category,
    double sentiment,
    String intent,
    List<String> entities,
    double confidence
) {}