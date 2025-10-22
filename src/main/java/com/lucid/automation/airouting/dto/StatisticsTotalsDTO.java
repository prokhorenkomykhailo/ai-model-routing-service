package com.lucid.automation.airouting.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Totals section for message statistics
 * @author vudu
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StatisticsTotalsDTO {

    /**
     * Total number of messages
     */
    private Long messageCount;

    /**
     * Number of processed messages
     */
    private Long processedCount;

    /**
     * Number of unprocessed messages
     */
    private Long unprocessedCount;

    /**
     * Number of threaded messages (messages in threads)
     */
    private Long threadedCount;

    /**
     * Number of unique users
     */
    private Long uniqueUsers;
}
