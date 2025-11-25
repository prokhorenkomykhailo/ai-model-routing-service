package com.lucid.automation.airouting.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

/**
 * Configuration for asynchronous task execution.
 * Configures a dedicated thread pool for async operations like Redis batch saves.
 *
 * @author vudu
 */
@Configuration
public class AsyncConfig implements AsyncConfigurer {

    private static final Logger logger = LoggerFactory.getLogger(AsyncConfig.class);

    /**
     * Configure the task executor for @Async methods.
     * Uses a thread pool with configurable size to handle async Redis operations.
     */
    @Bean(name = "taskExecutor")
    @Override
    public Executor getAsyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

        // Core pool size: minimum threads to keep alive
        executor.setCorePoolSize(4);

        // Max pool size: maximum threads when queue is full
        executor.setMaxPoolSize(8);

        // Queue capacity: tasks to queue before creating new threads
        executor.setQueueCapacity(100);

        // Thread name prefix for identification in logs
        executor.setThreadNamePrefix("AsyncRedis-");

        // Wait for tasks to complete on shutdown
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60);

        executor.initialize();

        logger.info("🔧 [ASYNC-CONFIG] Task executor initialized: core={}, max={}, queue={}",
            executor.getCorePoolSize(), executor.getMaxPoolSize(), executor.getQueueCapacity());

        return executor;
    }

    /**
     * Handle uncaught exceptions in async methods.
     */
    @Override
    public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
        return (throwable, method, params) -> {
            logger.error("❌ [ASYNC-ERROR] Uncaught exception in async method '{}': {}",
                method.getName(), throwable.getMessage(), throwable);
        };
    }

    /**
     * Dedicated executor for Redis async operations (marking messages as processed).
     * Higher capacity than default executor to handle high-throughput Redis write operations.
     * This prevents blocking Kafka consumer threads during Redis cleanup.
     *
     * @return Executor for Redis async operations
     */
    @Bean(name = "redisAsyncExecutor")
    public Executor redisAsyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

        // Higher capacity for Redis operations
        executor.setCorePoolSize(10);  // More threads for Redis ops
        executor.setMaxPoolSize(50);   // Can scale up significantly
        executor.setQueueCapacity(1000); // Large queue for burst handling

        executor.setThreadNamePrefix("redis-async-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60);

        executor.initialize();

        logger.info("⚡ [REDIS-ASYNC-CONFIG] Redis async executor initialized: core={}, max={}, queue={}",
            executor.getCorePoolSize(), executor.getMaxPoolSize(), executor.getQueueCapacity());

        return executor;
    }
}
