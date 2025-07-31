// Entity for enrichment jobs, following Redis and Lombok conventions

package com.lucid.automation.airouting.model;

import lombok.Data;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.redis.core.RedisHash;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@RedisHash("enrichmentJob")
public class EnrichmentJob implements Serializable {
    @Id
    private String id;
    private String status;
    private String type;
    private String result;
    private Long createdAt;
    private Long updatedAt;

    // Progress and timing fields
    private Double progress; // 0.0 to 1.0
    private Long durationMs;
    private String startTime; // ISO-8601 string
    private String endTime;   // ISO-8601 string or null
    private String estimatedCompletionTime; // ISO-8601 string or null
    private Long estimatedTimeLeft;

    // Multi-tenant and user tracking fields
    @org.springframework.data.redis.core.index.Indexed
    private String userId;
    @org.springframework.data.redis.core.index.Indexed
    private String tenantId;
    private String tenantSchema;
}