package com.lucid.automation.airouting.dto;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public record TopicEnrichment(
    String title,
    String shortSummary,
    String summary,
    String suggestedAction,
    String clientOrSupplier,
    String deadline,
    UrgencyLevel urgency,
    String category,
    LocalDateTime startTime,
    LocalDateTime endTime,
    List<UserDTO> peopleInvolved,
    Map<String, String> summaryPerPerson,
    List<ConversationMessage> conversations,
    ReplyInfo reply,
    ForwardInfo forward
) {}