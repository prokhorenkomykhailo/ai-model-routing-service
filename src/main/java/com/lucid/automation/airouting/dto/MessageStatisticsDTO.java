package com.lucid.automation.airouting.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Top-level response DTO for message statistics
 * @author vudu
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MessageStatisticsDTO {

    /**
     * Tenant ID for which statistics were generated
     */
    private String tenantId;

    /**
     * Aggregated totals
     */
    private StatisticsTotalsDTO totals;

    /**
     * Detailed breakdowns (null if includeBreakdowns=false)
     */
    private StatisticsBreakdownsDTO breakdowns;

    /**
     * Metadata about the statistics generation
     */
    private StatisticsMetadataDTO metadata;
}
