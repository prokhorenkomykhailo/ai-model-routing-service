package com.lucid.automation.airouting.config;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.logging.LoggingSpanExporter;
import io.opentelemetry.exporter.logging.SystemOutLogRecordExporter;
import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.export.RetryPolicy;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;

@Configuration
public class OpenTelemetryConfig {

    private static final Logger logger = LoggerFactory.getLogger(OpenTelemetryConfig.class);

    @Value("${spring.application.name}")
    private String serviceName;

    @Value("${opentelemetry.exporter.otlp.endpoint:http://localhost:4317}")
    private String otlpEndpoint;

    @Value("${opentelemetry.sampler.probability:1.0}")
    private double samplerProbability;

    @Bean
    public OpenTelemetry openTelemetry() {
        Resource resource = Resource.getDefault()
                .merge(Resource.create(Attributes.builder()
                        .put("service.name", serviceName)
                        .put("service.version", "1.0.0")
                        .build()));

        SpanExporter spanExporter = getSpanExporter();

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(BatchSpanProcessor.builder(spanExporter).build())
                .setResource(resource)
                .setSampler(Sampler.traceIdRatioBased(samplerProbability))
                .build();

        SdkMeterProvider meterProvider;
        try {
            if (isDevelopmentEnvironment() || !isCollectorAvailable()) {
                logger.info("OpenTelemetry collector unavailable or running in dev mode, using no-op metrics");
                meterProvider = SdkMeterProvider.builder()
                        .setResource(resource)
                        .build();
            } else {
                OtlpGrpcMetricExporter metricExporter = OtlpGrpcMetricExporter.builder()
                        .setEndpoint(otlpEndpoint)
                        .setTimeout(Duration.ofSeconds(2))
                        .setRetryPolicy(defaultRetryPolicy())
                        .build();

                meterProvider = SdkMeterProvider.builder()
                        .registerMetricReader(PeriodicMetricReader.builder(metricExporter).build())
                        .setResource(resource)
                        .build();
            }
        } catch (Exception e) {
            System.err.println("Error configuring OTLP metric exporter, using no-op exporter: " + e.getMessage());
            meterProvider = SdkMeterProvider.builder()
                    .setResource(resource)
                    .build();
        }

        LogRecordExporter logRecordExporter = getLogRecordExporter();
        SdkLoggerProvider loggerProvider = SdkLoggerProvider.builder()
                .addLogRecordProcessor(BatchLogRecordProcessor.builder(logRecordExporter).build())
                .setResource(resource)
                .build();

        return OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setMeterProvider(meterProvider)
                .setLoggerProvider(loggerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();
    }

    private SpanExporter getSpanExporter() {
        try {
            if (isDevelopmentEnvironment() || !isCollectorAvailable()) {
                return LoggingSpanExporter.create();
            }
            return OtlpGrpcSpanExporter.builder()
                    .setEndpoint(otlpEndpoint)
                    .setTimeout(Duration.ofSeconds(2))
                    .setRetryPolicy(defaultRetryPolicy())
                    .build();
        } catch (Exception e) {
            System.err.println("Error configuring OTLP span exporter, falling back to logging: " + e.getMessage());
            return LoggingSpanExporter.create();
        }
    }

    private LogRecordExporter getLogRecordExporter() {
        try {
            if (isDevelopmentEnvironment() || !isCollectorAvailable()) {
                return SystemOutLogRecordExporter.create();
            }
            return OtlpGrpcLogRecordExporter.builder()
                    .setEndpoint(otlpEndpoint)
                    .setTimeout(Duration.ofSeconds(2))
                    .setRetryPolicy(defaultRetryPolicy())
                    .build();
        } catch (Exception e) {
            System.err.println("Error configuring OTLP log exporter, falling back to system out: " + e.getMessage());
            return SystemOutLogRecordExporter.create();
        }
    }

    private boolean isDevelopmentEnvironment() {
        String env = System.getenv("SPRING_PROFILES_ACTIVE");
        return env != null && (env.contains("dev") || env.contains("local") || env.contains("test"));
    }

    private boolean isCollectorAvailable() {
        try (Socket socket = new Socket()) {
            String host = otlpEndpoint.replace("http://", "").replace("https://", "").split(":")[0];
            int port = Integer.parseInt(otlpEndpoint.replace("http://", "").replace("https://", "").split(":")[1]);
            socket.connect(new InetSocketAddress(host, port), 1000);
            return true;
        } catch (Exception e) {
            logger.warn("OpenTelemetry collector not available at {}: {}", otlpEndpoint, e.getMessage());
            return false;
        }
    }

    private RetryPolicy defaultRetryPolicy() {
        return RetryPolicy.builder()
                .setInitialBackoff(Duration.ofMillis(500))
                .setMaxBackoff(Duration.ofSeconds(5))
                .setMaxAttempts(5)
                .build();
    }
}