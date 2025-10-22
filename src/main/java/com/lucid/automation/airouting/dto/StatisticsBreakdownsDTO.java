package com.lucid.automation.airouting.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Breakdowns section for message statistics
 * @author vudu
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StatisticsBreakdownsDTO {

    /**
     * Breakdown by workspace
     */
    private List<BreakdownItemDTO> byWorkspace;

    /**
     * Breakdown by channel
     */
    private List<BreakdownItemDTO> byChannel;

    /**
     * Breakdown by source (Slack, Gmail, etc.)
     */
    private List<BreakdownItemDTO> bySource;
}
