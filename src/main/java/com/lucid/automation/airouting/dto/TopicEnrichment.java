package com.lucid.automation.airouting.dto;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public record TopicEnrichment(
    String title,
    String shortSummary,
    String fullSummary,
    String suggestedAction,
    String clientOrSupplier,
    String deadline,
    UrgencyLevel urgency,
    String category,
    String subCategory,
    LocalDateTime startTime,
    LocalDateTime endTime,
    String periodStartDate,
    String periodEndDate,
    String latestMessageDate,
    List<UserDTO> peopleInvolved,
    List<SummaryPerPerson> summaryPerPerson,
    Map<String, String> lastMessageDatePerPerson,
    List<ConversationMessage> conversations,
    List<SuggestedReply> suggestedReplies,
    ForwardInfo suggestedForwardRecipient
) {}