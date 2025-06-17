package com.lucid.automation.airouting.dto;

import java.util.List;
import java.util.Map;

public record ConversationEnrichment(
    List<TopicEnrichment> topics,
    List<ParticipantInsight> participants,
    List<MessageEnrichment> messages,
    Map<String, Object> metadata
) {}