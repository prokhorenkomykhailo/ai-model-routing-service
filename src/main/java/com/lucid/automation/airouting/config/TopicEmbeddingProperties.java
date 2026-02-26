package com.lucid.automation.airouting.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "topic.embedding")
public class TopicEmbeddingProperties {

    private String metadataTopic = "ai-topic-metadata";
    private String dlqTopic = "ai-topic-embeddings-dlq";

    private String provider = "gemini";
    private String geminiModel = "gemini-embedding-001";
    private int dimension = 768;

    private Pgvector pgvector = new Pgvector();

    public String getMetadataTopic() {
        return metadataTopic;
    }

    public void setMetadataTopic(String metadataTopic) {
        this.metadataTopic = metadataTopic;
    }

    public String getDlqTopic() {
        return dlqTopic;
    }

    public void setDlqTopic(String dlqTopic) {
        this.dlqTopic = dlqTopic;
    }

    public String getProvider() {
        return provider;
    }

    public void setProvider(String provider) {
        this.provider = provider;
    }

    public String getGeminiModel() {
        return geminiModel;
    }

    public void setGeminiModel(String geminiModel) {
        this.geminiModel = geminiModel;
    }

    public int getDimension() {
        return dimension;
    }

    public void setDimension(int dimension) {
        this.dimension = dimension;
    }

    public Pgvector getPgvector() {
        return pgvector;
    }

    public void setPgvector(Pgvector pgvector) {
        this.pgvector = pgvector;
    }

    public static class Pgvector {
        private String jdbcUrl = "";
        private String username = "";
        private String password = "";
        private String schema = "public";
        private String table = "topic_embeddings";
        private boolean autoDdl = true;
        private int searchTopKDefault = 10;

        public String getJdbcUrl() {
            return jdbcUrl;
        }

        public void setJdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getSchema() {
            return schema;
        }

        public void setSchema(String schema) {
            this.schema = schema;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public boolean isAutoDdl() {
            return autoDdl;
        }

        public void setAutoDdl(boolean autoDdl) {
            this.autoDdl = autoDdl;
        }

        public int getSearchTopKDefault() {
            return searchTopKDefault;
        }

        public void setSearchTopKDefault(int searchTopKDefault) {
            this.searchTopKDefault = searchTopKDefault;
        }
    }
}
