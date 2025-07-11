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
