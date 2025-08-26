package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.config.RoutingConfig;
import com.lucid.automation.airouting.model.AITaskType;
import com.lucid.automation.airouting.provider.AIProvider;
import com.lucid.automation.airouting.provider.AIProviderFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.trace.Tracer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests for AIProviderRouterService
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AIProviderRouterServiceTest {

    @Mock
    private AIProviderFactory providerFactory;

    @Mock
    private AIProviderHealthChecker healthChecker;

    @Mock
    private TokenAvailabilityService tokenAvailabilityService;

    @Mock
    private RoutingConfig routingConfig;

    @Mock
    private Tracer tracer;

    @Mock
    private io.opentelemetry.api.trace.SpanBuilder spanBuilder;

    @Mock
    private io.opentelemetry.api.trace.Span span;

    @Mock
    private AIProvider geminiProvider;

    @Mock
    private AIProvider openaiProvider;

    private MeterRegistry meterRegistry;
    private AIProviderRouterService routerService;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        routerService = new AIProviderRouterService(
            providerFactory, healthChecker, tokenAvailabilityService,
            routingConfig, meterRegistry, tracer);

        // Setup provider mocks
        when(geminiProvider.getProviderId()).thenReturn("geminiProvider");
        when(geminiProvider.isAvailable()).thenReturn(true);
        when(openaiProvider.getProviderId()).thenReturn("openaiProvider");
        when(openaiProvider.isAvailable()).thenReturn(true);

        // Setup tracing mocks
        when(tracer.spanBuilder(anyString())).thenReturn(spanBuilder);
        when(spanBuilder.setAttribute(anyString(), anyString())).thenReturn(spanBuilder);
        when(spanBuilder.startSpan()).thenReturn(span);
    }

    @Test
    void testSelectProvider_PreferredProviderAvailable() {
        // Given
        String tenantId = "tenant-123";
        String preferredProvider = "geminiProvider";
        AITaskType taskType = AITaskType.TEXT_QUERY;

        when(providerFactory.getProvider(preferredProvider)).thenReturn(geminiProvider);
        when(healthChecker.isProviderHealthy(preferredProvider)).thenReturn(true);
        when(tokenAvailabilityService.isTokenAvailable(tenantId)).thenReturn(true);

        // When
        AIProvider result = routerService.selectProvider(taskType, tenantId, preferredProvider);

        // Then
        assertEquals(geminiProvider, result);
        verify(providerFactory).getProvider(preferredProvider);
        verify(healthChecker).isProviderHealthy(preferredProvider);
        verify(tokenAvailabilityService).isTokenAvailable(tenantId);
    }

    @Test
    void testSelectProvider_PreferredProviderUnavailable_FallbackToTaskSpecific() {
        // Given
        String tenantId = "tenant-123";
        String preferredProvider = "geminiProvider";
        String taskProvider = "openaiProvider";
        AITaskType taskType = AITaskType.TEXT_QUERY;

        // Preferred provider unavailable
        when(providerFactory.getProvider(preferredProvider)).thenReturn(geminiProvider);
        when(healthChecker.isProviderHealthy(preferredProvider)).thenReturn(false);

        // Task-specific provider available
        when(routingConfig.getProviderForTask(taskType)).thenReturn(taskProvider);
        when(providerFactory.getProvider(taskProvider)).thenReturn(openaiProvider);
        when(healthChecker.isProviderHealthy(taskProvider)).thenReturn(true);
        when(tokenAvailabilityService.isTokenAvailable(tenantId)).thenReturn(true);

        when(tracer.spanBuilder(any())).thenReturn(mock(io.opentelemetry.api.trace.SpanBuilder.class));
        when(tracer.spanBuilder(any()).setAttribute(any(String.class), any(String.class)))
            .thenReturn(mock(io.opentelemetry.api.trace.SpanBuilder.class));
        when(tracer.spanBuilder(any()).startSpan()).thenReturn(mock(io.opentelemetry.api.trace.Span.class));

        // When
        AIProvider result = routerService.selectProvider(taskType, tenantId, preferredProvider);

        // Then
        assertEquals(openaiProvider, result);
        verify(routingConfig).getProviderForTask(taskType);
    }

    @Test
    void testSelectProvider_FallbackToDefault() {
        // Given
        String tenantId = "tenant-123";
        String defaultProvider = "geminiProvider";
        AITaskType taskType = AITaskType.TEXT_QUERY;

        // No task-specific provider
        when(routingConfig.getProviderForTask(taskType)).thenReturn(null);

        // Default provider available
        when(routingConfig.getDefaultProvider()).thenReturn(defaultProvider);
        when(providerFactory.getProvider(defaultProvider)).thenReturn(geminiProvider);
        when(healthChecker.isProviderHealthy(defaultProvider)).thenReturn(true);
        when(tokenAvailabilityService.isTokenAvailable(tenantId)).thenReturn(true);

        when(tracer.spanBuilder(any())).thenReturn(mock(io.opentelemetry.api.trace.SpanBuilder.class));
        when(tracer.spanBuilder(any()).setAttribute(any(String.class), any(String.class)))
            .thenReturn(mock(io.opentelemetry.api.trace.SpanBuilder.class));
        when(tracer.spanBuilder(any()).startSpan()).thenReturn(mock(io.opentelemetry.api.trace.Span.class));

        // When
        AIProvider result = routerService.selectProvider(taskType, tenantId);

        // Then
        assertEquals(geminiProvider, result);
        verify(routingConfig).getDefaultProvider();
    }

    @Test
    void testSelectProvider_FallbackToAnyAvailable() {
        // Given
        String tenantId = "tenant-123";
        AITaskType taskType = AITaskType.TEXT_QUERY;

        // No task-specific or default provider available
        when(routingConfig.getProviderForTask(taskType)).thenReturn(null);
        when(routingConfig.getDefaultProvider()).thenReturn(null);

        // Any available provider
        Map<String, AIProvider> allProviders = Map.of(
            "geminiProvider", geminiProvider,
            "openaiProvider", openaiProvider
        );
        when(providerFactory.getAllProviders()).thenReturn(allProviders);
        when(healthChecker.isProviderHealthy("geminiProvider")).thenReturn(true);
        when(healthChecker.isProviderHealthy("openaiProvider")).thenReturn(false);
        when(tokenAvailabilityService.isTokenAvailable(tenantId)).thenReturn(true);

        when(tracer.spanBuilder(any())).thenReturn(mock(io.opentelemetry.api.trace.SpanBuilder.class));
        when(tracer.spanBuilder(any()).setAttribute(any(String.class), any(String.class)))
            .thenReturn(mock(io.opentelemetry.api.trace.SpanBuilder.class));
        when(tracer.spanBuilder(any()).startSpan()).thenReturn(mock(io.opentelemetry.api.trace.Span.class));

        // When
        AIProvider result = routerService.selectProvider(taskType, tenantId);

        // Then
        assertEquals(geminiProvider, result);
    }

    @Test
    void testSelectProvider_NoProvidersAvailable() {
        // Given
        String tenantId = "tenant-123";
        AITaskType taskType = AITaskType.TEXT_QUERY;

        // No providers available
        when(routingConfig.getProviderForTask(taskType)).thenReturn(null);
        when(routingConfig.getDefaultProvider()).thenReturn(null);
        when(providerFactory.getAllProviders()).thenReturn(Map.of());

        when(tracer.spanBuilder(any())).thenReturn(mock(io.opentelemetry.api.trace.SpanBuilder.class));
        when(tracer.spanBuilder(any()).setAttribute(any(String.class), any(String.class)))
            .thenReturn(mock(io.opentelemetry.api.trace.SpanBuilder.class));
        when(tracer.spanBuilder(any()).startSpan()).thenReturn(mock(io.opentelemetry.api.trace.Span.class));

        // When & Then
        assertThrows(AIProviderRouterService.NoAvailableProviderException.class, () ->
            routerService.selectProvider(taskType, tenantId));
    }

    @Test
    void testSelectProvider_TokensUnavailable() {
        // Given
        String tenantId = "tenant-123";
        String preferredProvider = "geminiProvider";
        AITaskType taskType = AITaskType.TEXT_QUERY;

        when(providerFactory.getProvider(preferredProvider)).thenReturn(geminiProvider);
        when(healthChecker.isProviderHealthy(preferredProvider)).thenReturn(true);
        when(tokenAvailabilityService.isTokenAvailable(tenantId)).thenReturn(false);

        // No other providers available
        when(routingConfig.getProviderForTask(taskType)).thenReturn(null);
        when(routingConfig.getDefaultProvider()).thenReturn(null);
        when(providerFactory.getAllProviders()).thenReturn(Map.of());

        when(tracer.spanBuilder(any())).thenReturn(mock(io.opentelemetry.api.trace.SpanBuilder.class));
        when(tracer.spanBuilder(any()).setAttribute(any(String.class), any(String.class)))
            .thenReturn(mock(io.opentelemetry.api.trace.SpanBuilder.class));
        when(tracer.spanBuilder(any()).startSpan()).thenReturn(mock(io.opentelemetry.api.trace.Span.class));

        // When & Then
        assertThrows(AIProviderRouterService.NoAvailableProviderException.class, () ->
            routerService.selectProvider(taskType, tenantId, preferredProvider));
    }

    @Test
    void testGetProviderStats() {
        // Given
        Map<String, AIProvider> allProviders = Map.of(
            "geminiProvider", geminiProvider,
            "openaiProvider", openaiProvider
        );
        when(providerFactory.getAllProviders()).thenReturn(allProviders);
        when(healthChecker.isProviderHealthy("geminiProvider")).thenReturn(true);
        when(healthChecker.isProviderHealthy("openaiProvider")).thenReturn(false);

        // When
        AIProviderRouterService.ProviderStats stats = routerService.getProviderStats();

        // Then
        assertEquals(2, stats.getTotalProviders());
        assertEquals(1, stats.getHealthyProviders());
        assertTrue(stats.getProviderHealths().get("geminiProvider"));
        assertFalse(stats.getProviderHealths().get("openaiProvider"));
    }

    @Test
    void testProviderStats_ToString() {
        // Given
        Map<String, Boolean> healthMap = Map.of("gemini", true, "openai", false);
        AIProviderRouterService.ProviderStats stats =
            new AIProviderRouterService.ProviderStats(2, 1, healthMap);

        // When
        String result = stats.toString();

        // Then
        assertTrue(result.contains("total=2"));
        assertTrue(result.contains("healthy=1"));
        assertTrue(result.contains("healths="));
    }

    @Test
    void testNoAvailableProviderException() {
        // Given
        String message = "No providers available";

        // When
        AIProviderRouterService.NoAvailableProviderException exception =
            new AIProviderRouterService.NoAvailableProviderException(message);

        // Then
        assertEquals(message, exception.getMessage());
    }
}
