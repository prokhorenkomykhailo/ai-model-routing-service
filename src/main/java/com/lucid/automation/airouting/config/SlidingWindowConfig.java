package com.lucid.automation.airouting.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration properties for sliding window processing.
 * Supports both traditional message-count-based and token-based processing.
 */
@Configuration
@ConfigurationProperties(prefix = "sliding.window")
public class SlidingWindowConfig {

    /**
     * Default configuration for traditional message-count-based processing.
     */
    private Default defaultConfig = new Default();

    /**
     * Token-based processing configuration.
     */
    private Token token = new Token();

    /**
     * Cleanup configuration.
     */
    private Cleanup cleanup = new Cleanup();

    // Getters and setters
    public Default getDefault() { return defaultConfig; }
    public void setDefault(Default defaultConfig) { this.defaultConfig = defaultConfig; }

    public Token getToken() { return token; }
    public void setToken(Token token) { this.token = token; }

    public Cleanup getCleanup() { return cleanup; }
    public void setCleanup(Cleanup cleanup) { this.cleanup = cleanup; }

    /**
     * Default sliding window configuration.
     */
    public static class Default {
        private Batch batch = new Batch();
        private Overlap overlap = new Overlap();

        public Batch getBatch() { return batch; }
        public void setBatch(Batch batch) { this.batch = batch; }

        public Overlap getOverlap() { return overlap; }
        public void setOverlap(Overlap overlap) { this.overlap = overlap; }

        public static class Batch {
            private int size = 50;

            public int getSize() { return size; }
            public void setSize(int size) { this.size = size; }
        }

        public static class Overlap {
            private int percentage = 20;

            public int getPercentage() { return percentage; }
            public void setPercentage(int percentage) { this.percentage = percentage; }
        }
    }

    /**
     * Token-based sliding window configuration.
     */
    public static class Token {
        private boolean enabled = true;
        private Max max = new Max();
        private Min min = new Min();
        private Timeout timeout = new Timeout();
        private Overlap overlap = new Overlap();
        private Fallback fallback = new Fallback();

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }

        public Max getMax() { return max; }
        public void setMax(Max max) { this.max = max; }

        public Min getMin() { return min; }
        public void setMin(Min min) { this.min = min; }

        public Timeout getTimeout() { return timeout; }
        public void setTimeout(Timeout timeout) { this.timeout = timeout; }

        public Overlap getOverlap() { return overlap; }
        public void setOverlap(Overlap overlap) { this.overlap = overlap; }

        public Fallback getFallback() { return fallback; }
        public void setFallback(Fallback fallback) { this.fallback = fallback; }

        public static class Max {
            private int tokens = 4000;

            public int getTokens() { return tokens; }
            public void setTokens(int tokens) { this.tokens = tokens; }
        }

        public static class Min {
            private int tokens = 100;

            public int getTokens() { return tokens; }
            public void setTokens(int tokens) { this.tokens = tokens; }
        }

        public static class Timeout {
            private int seconds = 300; // 5 minutes

            public int getSeconds() { return seconds; }
            public void setSeconds(int seconds) { this.seconds = seconds; }
        }

        public static class Overlap {
            private int percentage = 20;
            private int tokens = 200;

            public int getPercentage() { return percentage; }
            public void setPercentage(int percentage) { this.percentage = percentage; }

            public int getTokens() { return tokens; }
            public void setTokens(int tokens) { this.tokens = tokens; }
        }

        public static class Fallback {
            private Message message = new Message();

            public Message getMessage() { return message; }
            public void setMessage(Message message) { this.message = message; }

            public static class Message {
                private int count = 50;

                public int getCount() { return count; }
                public void setCount(int count) { this.count = count; }
            }
        }
    }

    /**
     * Cleanup configuration.
     */
    public static class Cleanup {
        private boolean enabled = true;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
    }
}
