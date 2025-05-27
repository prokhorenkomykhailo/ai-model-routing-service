# Lucid AI Routing Service

This service provides AI model routing and orchestration capabilities for the Lucid platform.

## Features

- Multi-provider AI routing (OpenAI, Gemini, LangChain)
- Conversation analysis and enrichment
- Message categorization and sentiment analysis
- Participant behavior insights
- Topic generation and entity extraction

## Configuration

### Google Gemini Provider

The service now uses the official Google Generative AI library for Gemini integration.

#### Environment Variables

Set the following environment variable to configure the Gemini API:

```bash
export GOOGLE_API_KEY=your_gemini_api_key_here
```

#### Application Configuration

The Gemini provider is configured in `application.yml`:

```yaml
ai:
  routing:
    default-provider: gemini
  providers:
    gemini:
      type: gemini
      model: gemini-2.0-flash
      enabled: true
```

#### Model Selection

Supported Gemini models:
- `gemini-2.0-flash` (default, recommended)
- `gemini-pro`
- `gemini-pro-vision`

### Other Providers

- **OpenAI**: Configure with `OPENAI_API_KEY` environment variable
- **LangChain**: Configure according to LangChain documentation

## Development

### Building

```bash
mvn clean compile
```

### Running Tests

```bash
mvn test
```

### Running the Service

```bash
mvn spring-boot:run
```

## API Documentation

The service provides Swagger UI documentation at `/swagger-ui.html` when running.

## Dependencies

- Spring Boot 3.4.5
- Google Generative AI Client 1.0.0
- Jackson for JSON processing
- JWT for authentication