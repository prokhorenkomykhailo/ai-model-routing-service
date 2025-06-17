package com.lucid.automation.airouting.dto;

public record ForwardInfo(
    String channel,
    String to,
    String subject,
    String body
) {}