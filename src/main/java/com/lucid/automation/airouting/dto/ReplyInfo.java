package com.lucid.automation.airouting.dto;

import java.util.List;

public record ReplyInfo(
    String channel,
    String mode,
    String to,
    List<String> cc,
    String threadId,
    String subject,
    String body
) {}