# Pipeline Architecture - Separated Packages

## Overview

The pipeline architecture has been refactored into separate packages for better organization and maintainability:

- **Ingestion Pipeline** (`com.lucid.automation.airouting.pipeline.ingestion`) - Processes incoming Kafka messages
- **Post-Processing Pipeline** (`com.lucid.automation.airouting.pipeline.postprocessing`) - Processes AI responses

## Package Structure

```
pipeline/
├── ProcessingResult.java              # Common result type
├── config/                            # Common configuration
│   └── PipelineConfiguration.java
├── context/                           # Common context utilities
│   └── MessageProcessingContext.java  # Context for ingestion pipeline
├── ingestion/                         # Ingestion Pipeline
│   ├── MessageProcessor.java          # Interface for ingestion processors
│   ├── IngestionPipelineOrchestrator.java
│   └── processors/
│       ├── ValidationProcessor.java
│       ├── MessageStorageProcessor.java
│       ├── UserProcessingProcessor.java
│       └── WorkspaceProcessingProcessor.java
└── postprocessing/                    # Post-Processing Pipeline
    ├── PostProcessingPipelineOrchestrator.java
    ├── PostProcessingPipelineFactory.java
    ├── PostProcessingPipelineResult.java
    ├── PostProcessingContext.java
    └── step/
        ├── PipelineStep.java
        ├── PipelineStepResult.java
        ├── InputValidationStep.java
        ├── MessageConversionStep.java
        ├── TimestampProcessingStep.java
        ├── ConversationEnrichmentStep.java
        ├── TopicEnrichmentStep.java
        └── ResponseBuildingStep.java
```

## Ingestion Pipeline

### Purpose
Processes incoming messages from Kafka topics and stores them in Redis, along with user and workspace information.

### Processors (in order)
1. **ValidationProcessor** (order: 10) - Validates message structure and data
2. **MessageStorageProcessor** (order: 20) - Stores message to Redis
3. **UserProcessingProcessor** (order: 30) - Creates/updates user information
4. **WorkspaceProcessingProcessor** (order: 40) - Creates/updates workspace information

### Usage
```java
@Autowired
private IngestionPipelineOrchestrator orchestrator;

ProcessingResult result = orchestrator.processMessage(ingestionEvent);
```

## Post-Processing Pipeline

### Purpose
Processes AI responses and enriches them with additional context and formatting.

### Steps (in order)
1. **InputValidationStep** (order: 10) - Validates input data
2. **MessageConversionStep** (order: 20) - Converts message formats
3. **TimestampProcessingStep** (order: 30) - Processes timestamps
4. **ConversationEnrichmentStep** (order: 40) - Enriches conversations
5. **TopicEnrichmentStep** (order: 50) - Enriches topics
6. **ResponseBuildingStep** (order: 90) - Builds final response

### Usage
```java
@Autowired
private PostProcessingPipelineOrchestrator orchestrator;

PostProcessingPipelineResult result = orchestrator.processResponse(responseMap);
```

## Key Benefits

1. **Separation of Concerns** - Each pipeline handles distinct responsibilities
2. **Modularity** - Processors/steps can be easily added, removed, or reordered
3. **Testability** - Individual processors can be unit tested independently
4. **Maintainability** - Clear package boundaries make code easier to navigate
5. **Extensibility** - New pipelines can be added without affecting existing ones

## Migration Notes

- `IngestionConsumer` now uses `IngestionPipelineOrchestrator` instead of direct service calls
- `PostProcessingConsumer` can be updated to use the post-processing pipeline
- All processor/step classes are Spring components and will be auto-discovered
- The pipeline orchestrators automatically order processors/steps by their `getOrder()` method
