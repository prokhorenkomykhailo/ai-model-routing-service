package com.lucid.automation.airouting.dto;

import java.time.LocalDateTime;
import java.util.List;

public record SummaryPerPerson(
    String id,
    String username,
    String displayName,
    String imageUrl,
    String summary,
    String role,
    Integer messageCount,
    LocalDateTime firstMessageDate,
    LocalDateTime lastMessageDate,
    List<String> keyContributions,
    List<String> actionItems
) {}
