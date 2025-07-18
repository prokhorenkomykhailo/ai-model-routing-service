# Pipeline Configuration for Docker Environment

This document explains the pipeline configuration setup for the Docker environment of the Lucid AI Routing Service.

## Configuration Files

### 1. Profile Configuration
- **Default Profile**: `docker` (set in `pom.xml`)
- **Active Configuration**: `application-docker.yml`
- **Pipeline Config**: `pipeline-config-docker.yml`

### 2. Pipeline Configuration Properties

The pipeline is configured using the `lucid.post-processing.pipeline` property prefix in `application-docker.yml` and `pipeline-config-docker.yml`.

#### Main Pipeline Settings

```yaml
lucid:
  post-processing:
    pipeline:
      enabled: true                    # Enable/disable the entire pipeline
      type: STANDARD                   # Pipeline type: STANDARD, COMPREHENSIVE, MINIMAL, CUSTOM
      continueOnFailure: true          # Continue processing even if one step fails
      maxExecutionTimeMs: 45000        # Maximum execution time for the entire pipeline
      detailedLogging: true            # Enable detailed logging for debugging
      metricsEnabled: true             # Enable pipeline metrics collection
```

#### Pipeline Types

1. **STANDARD** (Recommended for Docker)
   - Context validation
   - Tenant mapping
   - User enrichment
   - Response formatting

2. **COMPREHENSIVE**
   - All STANDARD steps plus:
   - Channel enrichment
   - Topic enrichment (with AI)
   - Sentiment analysis
   - Audit logging

3. **MINIMAL**
   - Context validation
   - Response formatting

4. **CUSTOM**
   - Specify custom steps via `customSteps` property
   - Available steps: `CONTEXT_VALIDATION`, `TENANT_MAPPING`, `USER_ENRICHMENT`, `CHANNEL_ENRICHMENT`, `TOPIC_ENRICHMENT`, `SENTIMENT_ANALYSIS`, `RESPONSE_FORMATTING`, `AUDIT_LOGGING`

#### Individual Step Configuration

Each pipeline step can be configured individually:

```yaml
pipeline:
  steps:
    context-validation:
      enabled: true
      timeout-ms: 5000
      strict-mode: false
      
    user-enrichment:
      enabled: true
      timeout-ms: 10000
      batch-size: 10
      retry-attempts: 3
      
    topic-enrichment:
      enabled: true
      timeout-ms: 15000
      ai-enabled: true
      ai-provider: gemini
```

## Environment Variables

You can override any configuration using environment variables:

### Core Pipeline Settings
- `LUCID_PIPELINE_ENABLED` - Enable/disable pipeline (default: true)
- `LUCID_PIPELINE_TYPE` - Pipeline type (default: STANDARD)
- `LUCID_PIPELINE_CONTINUE_ON_FAILURE` - Continue on failure (default: true)
- `LUCID_PIPELINE_MAX_EXECUTION_TIME` - Max execution time in ms (default: 45000)

### Step-specific Settings
- `PIPELINE_CONTEXT_VALIDATION_ENABLED` - Enable context validation (default: true)
- `PIPELINE_USER_ENRICHMENT_ENABLED` - Enable user enrichment (default: true)
- `PIPELINE_TOPIC_AI_ENABLED` - Enable AI for topic enrichment (default: true)
- `PIPELINE_SENTIMENT_PROVIDER` - Sentiment analysis provider (default: gemini)

### Performance Tuning
- `PIPELINE_THREAD_POOL_SIZE` - Thread pool size (default: 5)
- `PIPELINE_QUEUE_CAPACITY` - Queue capacity (default: 50)
- `AI_ENRICHMENT_BATCH_SIZE` - Batch size for AI processing (default: 30)

## Docker Compose Example

```yaml
version: '3.8'
services:
  ai-routing-service:
    image: lucid/ai-routing-service:latest
    environment:
      - SPRING_PROFILES_ACTIVE=docker
      - LUCID_PIPELINE_ENABLED=true
      - LUCID_PIPELINE_TYPE=STANDARD
      - PIPELINE_USER_ENRICHMENT_ENABLED=true
      - PIPELINE_TOPIC_AI_ENABLED=true
      - GEMINI_API_KEY=${GEMINI_API_KEY}
      - REDIS_HOST=redis
      - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
    ports:
      - "8083:8083"
    depends_on:
      - redis
      - kafka
```

## Monitoring and Health Checks

The pipeline exposes several endpoints for monitoring:

- `/actuator/health` - General health check
- `/actuator/health/pipeline` - Pipeline-specific health check
- `/actuator/metrics` - Pipeline metrics
- `/actuator/info` - Application info

### Health Check Configuration

```yaml
management:
  health:
    pipeline:
      enabled: true
      threshold:
        success-rate: 0.8     # Minimum 80% success rate
        max-response-time: 30000  # Maximum 30 seconds response time
```

## Troubleshooting

### Common Issues

1. **Pipeline Disabled**
   ```
   Pipeline is disabled, skipping processing
   ```
   **Solution**: Set `LUCID_PIPELINE_ENABLED=true`

2. **Step Timeout**
   ```
   Step timeout exceeded: user-enrichment
   ```
   **Solution**: Increase timeout for specific step or overall pipeline

3. **AI Provider Errors**
   ```
   AI provider 'gemini' not available
   ```
   **Solution**: Check API key and network connectivity

### Debug Mode

Enable detailed logging for debugging:

```yaml
logging:
  level:
    com.lucid.automation.airouting.pipeline: DEBUG
    com.lucid.automation.airouting.consumer: DEBUG
```

Or via environment variable:
```bash
export LOGGING_LEVEL_COM_LUCID_AUTOMATION_AIROUTING_PIPELINE=DEBUG
```

## Configuration Classes

The configuration is bound to Java classes:

- `PipelineConfigurationProperties` - Main configuration properties
- `PipelineConfiguration` - Runtime configuration interface
- `PostProcessingConsumer` - Main consumer using the configuration

These classes use Spring Boot's `@ConfigurationProperties` to automatically bind YAML configuration to Java objects.
