package com.lucid.automation.airouting.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.StringUtils;

@Configuration
public class TopicEmbeddingPgvectorDataSourceConfig {

    private static final Logger logger = LoggerFactory.getLogger(TopicEmbeddingPgvectorDataSourceConfig.class);

    @Bean(name = "topicEmbeddingDataSource")
    @ConditionalOnExpression("T(org.springframework.util.StringUtils).hasText('${topic.embedding.pgvector.jdbc-url:}')")
    public DataSource topicEmbeddingDataSource(TopicEmbeddingProperties properties) {
        String jdbcUrl = properties.getPgvector().getJdbcUrl();
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(jdbcUrl);
        cfg.setDriverClassName("org.postgresql.Driver");
        if (StringUtils.hasText(properties.getPgvector().getUsername())) {
            cfg.setUsername(properties.getPgvector().getUsername());
        }
        if (StringUtils.hasText(properties.getPgvector().getPassword())) {
            cfg.setPassword(properties.getPgvector().getPassword());
        }

        // Many managed Postgres poolers (e.g., Supabase "transaction" pool mode) can break server-side prepared statements.
        // Force simple query mode and disable statement caches so vector search/upserts stay reliable.
        cfg.addDataSourceProperty("preferQueryMode", "simple");
        cfg.addDataSourceProperty("prepareThreshold", "0");
        cfg.addDataSourceProperty("preparedStatementCacheQueries", "0");
        cfg.addDataSourceProperty("preparedStatementCacheSizeMiB", "0");

        cfg.setMaximumPoolSize(5);
        cfg.setPoolName("topicEmbeddingPgvectorPool");
        logger.info("✅ Step4 pgvector datasource configured for {}", jdbcUrl);
        return new HikariDataSource(cfg);
    }

    @Bean(name = "topicEmbeddingJdbcTemplate")
    @ConditionalOnExpression("T(org.springframework.util.StringUtils).hasText('${topic.embedding.pgvector.jdbc-url:}')")
    public JdbcTemplate topicEmbeddingJdbcTemplate(@Qualifier("topicEmbeddingDataSource") DataSource ds) {
        return new JdbcTemplate(ds);
    }
}
