package com.lucid.automation.airouting.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO for message search/filter parameters
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MessageSearchDTO {
    
    private String tenantId;
    private String workspaceId;
    private String channelId;
    private String threadTs;
    private String userId;
    private String messageType;
    private String subtype;
    private Long startTime;
    private Long endTime;
    private String textContains;
    
    // Pagination parameters
    @Builder.Default
    private int page = 0;
    @Builder.Default
    private int size = 20;
    @Builder.Default
    private String sortBy = "ingestedAt";
    @Builder.Default
    private String sortDirection = "DESC";
}
