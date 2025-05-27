package com.lucid.automation.airouting.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

/**
 * DTO for message with content from data-storage-service
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MessageWithContentDTO {
    private UUID id;
    private String messageId;
    private String source;
    private String workspaceId;
    private String channelId;
    private String senderId;
    private String senderName;
    private String type;
    private String subtype;
    private String username;
    private UUID topicId;
    private String purpose;
    private String clientMsgId;
    private LocalDateTime messageTimestamp;
    private String groupId;
    private String content;
    private Map<String, Object> metadata;
    private String contentUrl;
    private Boolean isDeleted;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    private String category;
    private String sentiment;
    private Double confidenceScore;
    private Map<String, Object> enrichmentData;
}
