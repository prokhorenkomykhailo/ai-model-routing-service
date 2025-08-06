// Entity for enrichment jobs, following Redis and Lombok conventions

package com.lucid.automation.airouting.model;

import lombok.Data;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.TimeToLive;
import org.springframework.data.redis.core.index.Indexed;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@RedisHash("enrichmentJob")
public class EnrichmentJob implements Serializable {
    @Id
    private String id;
    @Indexed
    private String parentId; // Optional parent job ID for job hierarchy
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
    @Indexed
    private String userId;
    @Indexed
    private String tenantId;
    private String tenantSchema;

    // TTL (Time To Live) for Redis entries - 60 minutes (3600 seconds)
    // This ensures enrichment jobs are automatically cleaned up after 60 minutes
    // to prevent Redis memory buildup and maintain performance
    @TimeToLive(unit = TimeUnit.SECONDS)
    @Builder.Default
    private Long ttl = 3600L; // 60 minutes in seconds
}