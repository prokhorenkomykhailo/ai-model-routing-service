package com.lucid.automation.airouting.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * DTO for message with content from data-storage-service
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
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
    private String topicId; // Changed from UUID to String to match data storage service
    private String topicName;
    private String purpose;
    private String clientMsgId;
    private LocalDateTime messageTimestamp;
    private String contentUrl;
    private String category;
    private List<String> tags;
    private Map<String, Object> participants;
    private Map<String, Object> additionalMetadata;
    private boolean hasAttachments;
    private boolean isEnriched;
    private boolean isEncrypted;
    private String threadTs;
    private String groupId;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    private String content;
    
    // Additional fields for AI processing (may not be present in data storage responses)
    private Map<String, Object> metadata;
    private Boolean isDeleted;
    private String sentiment;
    private Double confidenceScore;
    private Map<String, Object> enrichmentData;
}
