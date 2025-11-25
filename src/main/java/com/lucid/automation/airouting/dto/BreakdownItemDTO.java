package com.lucid.automation.airouting.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Individual breakdown entry for statistics
 * @author vudu
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BreakdownItemDTO {

    /**
     * Identifier (workspace ID, channel ID, or null for source breakdown)
     */
    private String id;

    /**
     * Display name (workspace name, channel name, or source name)
     */
    private String name;

    /**
     * Total count for this item
     */
    private Long count;

    /**
     * Processed count (optional, only for workspace breakdown)
     */
    private Long processedCount;
}
