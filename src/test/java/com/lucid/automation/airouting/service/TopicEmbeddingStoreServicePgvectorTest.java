package com.lucid.automation.airouting.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucid.automation.airouting.config.TopicEmbeddingProperties;
import com.lucid.automation.airouting.dto.topic.TopicMetadata;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class TopicEmbeddingStoreServicePgvectorTest {

    @Test
    void upsertAndSearch_pgvector() {
        try (GenericContainer<?> pg = new GenericContainer<>(DockerImageName.parse("pgvector/pgvector:pg16"))
            .withEnv("POSTGRES_PASSWORD", "test")
            .withEnv("POSTGRES_USER", "test")
            .withEnv("POSTGRES_DB", "test")
            .withExposedPorts(5432)) {

            pg.start();

            String jdbcUrl = "jdbc:postgresql://" + pg.getHost() + ":" + pg.getMappedPort(5432) + "/test";

            HikariConfig cfg = new HikariConfig();
            cfg.setJdbcUrl(jdbcUrl);
            cfg.setUsername("test");
            cfg.setPassword("test");
            try (HikariDataSource ds = new HikariDataSource(cfg)) {
                JdbcTemplate jdbc = new JdbcTemplate(ds);

                TopicEmbeddingProperties props = new TopicEmbeddingProperties();
                props.setProvider("deterministic");
                props.setDimension(32);
                props.getPgvector().setJdbcUrl(jdbcUrl);
                props.getPgvector().setUsername("test");
                props.getPgvector().setPassword("test");
                props.getPgvector().setSchema("public");
                props.getPgvector().setTable("topic_embeddings");
                props.getPgvector().setAutoDdl(true);

                TopicEmbeddingStoreService service = new TopicEmbeddingStoreService(props, new ObjectMapper(), Optional.of(jdbc));

                TopicMetadata t1 = new TopicMetadata();
                t1.setTitle("EcoBloom Summer Campaign");
                t1.setSummary("Design and legal review timeline.");
                t1.setChannel("#campaign-briefs");
                t1.setExternalParty("EcoBloom");

                TopicMetadata t2 = new TopicMetadata();
                t2.setTitle("FitFusion Rebranding");
                t2.setSummary("Trademark and tagline approvals.");
                t2.setChannel("#project-updates");
                t2.setExternalParty("FitFusion");

                service.upsert("11111111-1111-1111-1111-111111111111", t1, "ws-test", "batch-test", "geminiProvider");
                service.upsert("22222222-2222-2222-2222-222222222222", t2, "ws-test", "batch-test", "geminiProvider");

                Integer count = jdbc.queryForObject("select count(*) from public.topic_embeddings where workspace_id='ws-test'", Integer.class);
                Assertions.assertEquals(2, count);

                var results = service.search("ws-test", t1, 2);
                Assertions.assertFalse(results.isEmpty());
                Assertions.assertEquals("11111111-1111-1111-1111-111111111111", results.get(0).topicId());
            }
        }
    }
}

