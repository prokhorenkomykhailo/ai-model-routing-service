package com.lucid.automation.airouting.dto;

import java.util.List;

public record SuggestedReply(
    String tone,
    String replyMethod,
    String recipientHandle,
    String channelName,
    String channelId,
    String threadId,
    String to,
    List<String> cc,
    String subject,
    String messageBody
) {}
