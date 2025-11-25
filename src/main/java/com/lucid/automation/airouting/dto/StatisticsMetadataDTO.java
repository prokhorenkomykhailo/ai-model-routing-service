package com.lucid.automation.airouting.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Metadata section for message statistics
 * @author vudu
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StatisticsMetadataDTO {

    /**
     * Timestamp when statistics were generated (epoch millis)
     */
    private Long generatedAt;

    /**
     * Retention window in days (7 days for Redis)
     */
    @Builder.Default
    private Integer retentionWindowDays = 7;

    /**
     * Data completeness indicator (complete, partial, or limited)
     */
    @Builder.Default
    private String dataCompleteness = "complete";

    /**
     * Start of queried time range (epoch millis, null if not filtered)
     */
    private Long queriedFrom;

    /**
     * End of queried time range (epoch millis, null if not filtered)
     */
    private Long queriedTo;

    /**
     * Whether breakdowns were truncated due to size limits
     */
    @Builder.Default
    private Boolean truncated = false;
}
