package com.lucid.automation.airouting.scheduler;

import com.lucid.automation.airouting.model.Message;
import com.lucid.automation.airouting.model.Workspace;
import com.lucid.automation.airouting.producer.AIMessageProducer;
import com.lucid.automation.airouting.service.SlidingWindowService;
import com.lucid.automation.airouting.service.WorkspaceService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Integration tests for MessageEnrichmentScheduler pre-check logic (SCRUM-393).
 * Tests the message availability check before enrichment cycles.
 *
 * Scenarios:
 * 1. Empty cache → SKIP-CYCLE (entire processAllWorkspaces is skipped)
 * 2. Populated cache → Normal processing (enrichment continues)
 * 3. Mixed cache state → Workspace-level processing
 * 4. Error handling → Graceful degradation
 *
 * @author vudu
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("MessageEnrichmentScheduler Pre-Check Integration Tests (SCRUM-393)")
class MessageEnrichmentSchedulerIntegrationTest {

    @Mock
    private AIMessageProducer aiMessageProducer;

    @Mock
    private WorkspaceService workspaceService;

    @Mock
    private SlidingWindowService slidingWindowService;

    @Mock
    private ApplicationContext applicationContext;

    private MessageEnrichmentScheduler scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new MessageEnrichmentScheduler(
            aiMessageProducer,
            workspaceService,
            slidingWindowService,
            applicationContext,
            1000,  // batchSize
            "public",  // defaultTenantSchema
            30,  // maxMessageAgeDays
            5,  // minRecentMessages
            4  // parallelThreads
        );
    }

    // ==================== SKIP-CYCLE TESTS ====================

    @Test
    @DisplayName("Should skip enrichment cycle when no messages across all workspaces")
    void shouldSkipEnrichmentCycleWhenNoMessagesExist() {
        // Given: Two workspaces with NO unprocessed messages
        Workspace ws1 = createWorkspace("ws-1", "tenant-1");
        Workspace ws2 = createWorkspace("ws-2", "tenant-2");
        List<Workspace> workspaces = List.of(ws1, ws2);

        when(workspaceService.getAllWorkspaces()).thenReturn(workspaces);

        // Both workspaces have no messages (shouldProcessWorkspace returns false)
        SlidingWindowService.ProcessingDecision noMessagesDecision =
            new SlidingWindowService.ProcessingDecision(false,
                SlidingWindowService.ProcessingDecision.Reason.INSUFFICIENT_CRITERIA,
                "No unprocessed messages");

        when(slidingWindowService.shouldProcessWorkspace(any()))
            .thenReturn(noMessagesDecision);

        // When: Scheduler runs
        scheduler.processMessageEnrichment();

        // Then: Pre-check finds total count = 0, so processAllWorkspaces is NOT called
        // Verify both workspaces were checked
        verify(slidingWindowService, times(2)).shouldProcessWorkspace(any());

        // Verify no message processing occurred (mocked methods not called to process)
        verify(aiMessageProducer, never()).scheduleAiProcessing(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    @DisplayName("Should proceed with enrichment when at least one workspace has messages")
    void shouldProceedWithEnrichmentWhenMessagesExist() {
        // Given: Two workspaces, first one has messages
        Workspace ws1 = createWorkspace("ws-1", "tenant-1");
        Workspace ws2 = createWorkspace("ws-2", "tenant-2");
        List<Workspace> workspaces = List.of(ws1, ws2);

        when(workspaceService.getAllWorkspaces()).thenReturn(workspaces);

        // ws1: has messages (should process)
        SlidingWindowService.ProcessingDecision processDecision =
            new SlidingWindowService.ProcessingDecision(true,
                SlidingWindowService.ProcessingDecision.Reason.SUFFICIENT_MESSAGES,
                "Found 50 unprocessed messages");

        // ws2: no messages (skip)
        SlidingWindowService.ProcessingDecision skipDecision =
            new SlidingWindowService.ProcessingDecision(false,
                SlidingWindowService.ProcessingDecision.Reason.INSUFFICIENT_CRITERIA,
                "No messages");

        when(slidingWindowService.shouldProcessWorkspace(ws1)).thenReturn(processDecision);
        when(slidingWindowService.shouldProcessWorkspace(ws2)).thenReturn(skipDecision);

        // Mock message processing for ws1
        when(slidingWindowService.processMessages(any(Workspace.class), anyInt(), anyInt(), any()))
            .thenReturn(50);

        // When: Scheduler runs
        scheduler.processMessageEnrichment();

        // Then: Pre-check finds total count = 1 (ws1 has messages), processing continues
        verify(slidingWindowService, times(2)).shouldProcessWorkspace(any());

        // Verify processing was initiated at least once (since at least one workspace has messages)
        verify(slidingWindowService, atLeastOnce()).processMessages(any(Workspace.class), anyInt(), anyInt(), any());
    }

    // ==================== MIXED WORKSPACE STATE TESTS ====================

    @Test
    @DisplayName("Should handle multiple workspaces with mixed message states")
    void shouldHandleMixedWorkspaceScenarios() {
        // Given: Three workspaces with different states
        Workspace ws1 = createWorkspace("ws-1", "tenant-1");  // has messages
        Workspace ws2 = createWorkspace("ws-2", "tenant-2");  // no messages
        Workspace ws3 = createWorkspace("ws-3", "tenant-3");  // has messages
        List<Workspace> workspaces = List.of(ws1, ws2, ws3);

        when(workspaceService.getAllWorkspaces()).thenReturn(workspaces);

        // Setup decisions for each workspace
        SlidingWindowService.ProcessingDecision decision1 =
            new SlidingWindowService.ProcessingDecision(true,
                SlidingWindowService.ProcessingDecision.Reason.SUFFICIENT_MESSAGES,
                "Found messages");

        SlidingWindowService.ProcessingDecision decision2 =
            new SlidingWindowService.ProcessingDecision(false,
                SlidingWindowService.ProcessingDecision.Reason.INSUFFICIENT_CRITERIA,
                "No messages");

        SlidingWindowService.ProcessingDecision decision3 =
            new SlidingWindowService.ProcessingDecision(true,
                SlidingWindowService.ProcessingDecision.Reason.TIME_THRESHOLD,
                "Time threshold met");

        when(slidingWindowService.shouldProcessWorkspace(ws1)).thenReturn(decision1);
        when(slidingWindowService.shouldProcessWorkspace(ws2)).thenReturn(decision2);
        when(slidingWindowService.shouldProcessWorkspace(ws3)).thenReturn(decision3);

        // Mock message processing
        when(slidingWindowService.processMessages(any(Workspace.class), anyInt(), anyInt(), any()))
            .thenReturn(25);

        // When: Scheduler runs
        scheduler.processMessageEnrichment();

        // Then: Pre-check passes (2 out of 3 workspaces have messages, total > 0)
        // Processing should continue
        verify(slidingWindowService, times(3)).shouldProcessWorkspace(any());
    }

    // ==================== ERROR HANDLING TESTS ====================

    @Test
    @DisplayName("Should handle SlidingWindowService errors gracefully")
    void shouldHandleSlidingWindowServiceErrorsGracefully() {
        // Given: Workspace setup but SlidingWindowService throws exception
        Workspace workspace = createWorkspace("ws-1", "tenant-1");
        when(workspaceService.getAllWorkspaces()).thenReturn(List.of(workspace));

        // shouldProcessWorkspace throws exception
        when(slidingWindowService.shouldProcessWorkspace(workspace))
            .thenThrow(new RuntimeException("SlidingWindowService error"));

        // When: Scheduler runs
        // Should NOT throw exception
        scheduler.processMessageEnrichment();

        // Then: Scheduler handles error gracefully
        verify(slidingWindowService, times(1)).shouldProcessWorkspace(any());
    }

    @Test
    @DisplayName("Should handle WorkspaceService errors gracefully")
    void shouldHandleWorkspaceServiceErrorsGracefully() {
        // Given: WorkspaceService throws exception
        when(workspaceService.getAllWorkspaces())
            .thenThrow(new RuntimeException("Failed to fetch workspaces"));

        // When: Scheduler runs
        // Should NOT throw exception
        scheduler.processMessageEnrichment();

        // Then: Error is handled, pre-check not performed
        verify(slidingWindowService, never()).shouldProcessWorkspace(any());
    }

    @Test
    @DisplayName("Should handle empty workspace list gracefully")
    void shouldHandleEmptyWorkspaceListGracefully() {
        // Given: No workspaces available
        when(workspaceService.getAllWorkspaces()).thenReturn(List.of());

        // When: Scheduler runs
        scheduler.processMessageEnrichment();

        // Then: Early exit without calling shouldProcessWorkspace
        verify(slidingWindowService, never()).shouldProcessWorkspace(any());
    }

    // ==================== COUNTER MAINTENANCE TESTS ====================

    @Test
    @DisplayName("Should maintain scheduler run counters across cycles")
    void shouldMaintainSchedulerCounterAcrossCycles() {
        // Given: Workspace with no messages
        Workspace workspace = createWorkspace("ws-1", "tenant-1");
        when(workspaceService.getAllWorkspaces()).thenReturn(List.of(workspace));

        SlidingWindowService.ProcessingDecision skipDecision =
            new SlidingWindowService.ProcessingDecision(false,
                SlidingWindowService.ProcessingDecision.Reason.INSUFFICIENT_CRITERIA,
                "No messages");
        when(slidingWindowService.shouldProcessWorkspace(workspace))
            .thenReturn(skipDecision);

        // Get initial counter value
        Object initialCountObj = ReflectionTestUtils.getField(scheduler, "totalSchedulerRuns");
        int initialCount = ((java.util.concurrent.atomic.AtomicInteger) initialCountObj).get();

        // When: Scheduler runs
        scheduler.processMessageEnrichment();

        // Then: Counter should be incremented
        Object newCountObj = ReflectionTestUtils.getField(scheduler, "totalSchedulerRuns");
        int newCount = ((java.util.concurrent.atomic.AtomicInteger) newCountObj).get();
        assert newCount > initialCount : "Counter should be incremented";
    }

    // ==================== HELPER METHODS ====================

    /**
     * Create a test workspace.
     */
    private Workspace createWorkspace(String name, String tenantId) {
        Workspace workspace = new Workspace();
        workspace.setId(UUID.randomUUID().toString());
        workspace.setName(name);
        workspace.setTenantId(tenantId);
        workspace.setTenantSchema("schema_" + tenantId);
        workspace.setDeemergeUserId(UUID.randomUUID().toString());
        workspace.setCreatedAt(Instant.now());
        workspace.setUpdatedAt(Instant.now());
        return workspace;
    }
}
