package com.lucid.automation.airouting.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request DTO for message statistics endpoint
 * @author vudu
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MessageStatisticsRequestDTO {

    /**
     * Tenant ID (required)
     */
    private String tenantId;

    /**
     * Start timestamp for temporal filtering (epoch millis, optional)
     */
    private Long from;

    /**
     * End timestamp for temporal filtering (epoch millis, optional)
     */
    private Long to;

    /**
     * Whether to include detailed breakdowns (default: true)
     */
    @Builder.Default
    private Boolean includeBreakdowns = true;
}
