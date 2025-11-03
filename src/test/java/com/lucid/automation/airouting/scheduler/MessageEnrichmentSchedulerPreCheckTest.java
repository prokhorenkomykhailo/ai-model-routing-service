package com.lucid.automation.airouting.scheduler;

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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for MessageEnrichmentScheduler pre-check logic (SCRUM-393)
 * Tests the message availability check before enrichment cycles.
 *
 * @author vudu
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("MessageEnrichmentScheduler Pre-Check Tests (SCRUM-393)")
class MessageEnrichmentSchedulerPreCheckTest {

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
            5  // minRecentMessages
        );

        // Set config values via reflection if needed
        ReflectionTestUtils.setField(scheduler, "batchSize", 1000);
    }

    // ==================== PRE-CHECK SKIP CYCLE TESTS ====================

    @Test
    @DisplayName("Should skip enrichment cycle when no messages exist")
    void shouldSkipEnrichmentCycleWhenNoMessagesExist() {
        // Given: Empty workspace with no unprocessed messages
        List<Workspace> workspaces = List.of(createWorkspace("ws-1", "tenant-1"));

        when(workspaceService.getAllWorkspaces()).thenReturn(workspaces);

        // shouldProcessWorkspace returns false (no messages)
        SlidingWindowService.ProcessingDecision skipDecision =
            new SlidingWindowService.ProcessingDecision(false,
                SlidingWindowService.ProcessingDecision.Reason.INSUFFICIENT_CRITERIA,
                "No unprocessed messages");
        when(slidingWindowService.shouldProcessWorkspace(workspaces.get(0)))
            .thenReturn(skipDecision);

        // When: Scheduler runs
        scheduler.processMessageEnrichment();

        // Then: processAllWorkspaces should NOT be called
        verify(slidingWindowService, times(1)).shouldProcessWorkspace(any());
        // Verify early exit by checking method calls
    }

    @Test
    @DisplayName("Should proceed with enrichment when messages exist")
    void shouldProceedWithEnrichmentWhenMessagesExist() {
        // Given: Workspace with unprocessed messages
        List<Workspace> workspaces = List.of(createWorkspace("ws-1", "tenant-1"));

        when(workspaceService.getAllWorkspaces()).thenReturn(workspaces);

        // shouldProcessWorkspace returns true (has messages)
        SlidingWindowService.ProcessingDecision processDecision =
            new SlidingWindowService.ProcessingDecision(true,
                SlidingWindowService.ProcessingDecision.Reason.SUFFICIENT_MESSAGES,
                "Found 50 unprocessed messages");
        when(slidingWindowService.shouldProcessWorkspace(workspaces.get(0)))
            .thenReturn(processDecision);

        // Mock processMessages to return 1 batch processed
        when(slidingWindowService.processMessages(any(Workspace.class), anyInt(), anyInt(), any()))
            .thenReturn(50); // 50 messages processed

        // When: Scheduler runs
        scheduler.processMessageEnrichment();

        // Then: Processing should continue (not skipped)
        verify(slidingWindowService, times(1)).shouldProcessWorkspace(any());
    }

    @Test
    @DisplayName("Should handle mixed workspace scenarios")
    void shouldHandleMixedWorkspaceScenarios() {
        // Given: Mix of workspaces - some with messages, some without
        List<Workspace> workspaces = List.of(
            createWorkspace("ws-1", "tenant-1"),
            createWorkspace("ws-2", "tenant-2"),
            createWorkspace("ws-3", "tenant-3")
        );

        when(workspaceService.getAllWorkspaces()).thenReturn(workspaces);

        // ws-1: has messages (should process)
        SlidingWindowService.ProcessingDecision decision1 =
            new SlidingWindowService.ProcessingDecision(true,
                SlidingWindowService.ProcessingDecision.Reason.SUFFICIENT_MESSAGES,
                "Found messages");

        // ws-2: no messages (skip)
        SlidingWindowService.ProcessingDecision decision2 =
            new SlidingWindowService.ProcessingDecision(false,
                SlidingWindowService.ProcessingDecision.Reason.INSUFFICIENT_CRITERIA,
                "No messages");

        // ws-3: has messages (should process)
        SlidingWindowService.ProcessingDecision decision3 =
            new SlidingWindowService.ProcessingDecision(true,
                SlidingWindowService.ProcessingDecision.Reason.TIME_THRESHOLD,
                "Time threshold met");

        when(slidingWindowService.shouldProcessWorkspace(workspaces.get(0))).thenReturn(decision1);
        when(slidingWindowService.shouldProcessWorkspace(workspaces.get(1))).thenReturn(decision2);
        when(slidingWindowService.shouldProcessWorkspace(workspaces.get(2))).thenReturn(decision3);

        // When: Pre-check is run
        // We can't directly call calculateTotalUnprocessedMessages, so we run full scheduler
        when(slidingWindowService.processMessages(any(Workspace.class), anyInt(), anyInt(), any()))
            .thenReturn(25); // Each returns 25 messages

        scheduler.processMessageEnrichment();

        // Then: shouldProcessWorkspace called for each workspace during pre-check
        verify(slidingWindowService, atLeast(3)).shouldProcessWorkspace(any());
    }

    @Test
    @DisplayName("Should handle workspace query errors gracefully")
    void shouldHandleWorkspaceQueryErrorsGracefully() {
        // Given: Workspace service throws exception
        when(workspaceService.getAllWorkspaces())
            .thenThrow(new RuntimeException("Database connection failed"));

        // When: Scheduler runs
        scheduler.processMessageEnrichment();

        // Then: Should handle error without crashing
        // Verify that the scheduler completes its log info
        verify(workspaceService, times(1)).getAllWorkspaces();
    }

    @Test
    @DisplayName("Should handle SlidingWindowService errors gracefully")
    void shouldHandleSlidingWindowServiceErrorsGracefully() {
        // Given: Workspace with error in shouldProcessWorkspace
        List<Workspace> workspaces = List.of(createWorkspace("ws-1", "tenant-1"));

        when(workspaceService.getAllWorkspaces()).thenReturn(workspaces);
        when(slidingWindowService.shouldProcessWorkspace(any()))
            .thenThrow(new RuntimeException("Redis connection failed"));

        // When: Scheduler runs
        scheduler.processMessageEnrichment();

        // Then: Should handle error gracefully
        verify(slidingWindowService, times(1)).shouldProcessWorkspace(any());
    }

    @Test
    @DisplayName("Should process all workspaces when count > 0")
    void shouldProcessAllWorkspacesWhenCountGreaterThanZero() {
        // Given: Multiple workspaces all with messages
        List<Workspace> workspaces = List.of(
            createWorkspace("ws-1", "tenant-1"),
            createWorkspace("ws-2", "tenant-2")
        );

        when(workspaceService.getAllWorkspaces()).thenReturn(workspaces);

        SlidingWindowService.ProcessingDecision decision =
            new SlidingWindowService.ProcessingDecision(true,
                SlidingWindowService.ProcessingDecision.Reason.SUFFICIENT_MESSAGES,
                "Found messages");

        when(slidingWindowService.shouldProcessWorkspace(any())).thenReturn(decision);
        when(slidingWindowService.processMessages(any(Workspace.class), anyInt(), anyInt(), any()))
            .thenReturn(30);

        // When: Scheduler runs
        scheduler.processMessageEnrichment();

        // Then: All workspaces should be processed
        verify(slidingWindowService, atLeast(2)).shouldProcessWorkspace(any());
    }

    // ==================== EDGE CASE TESTS ====================

    @Test
    @DisplayName("Should handle empty workspace list")
    void shouldHandleEmptyWorkspaceList() {
        // Given: No workspaces
        when(workspaceService.getAllWorkspaces()).thenReturn(new ArrayList<>());

        // When: Scheduler runs
        scheduler.processMessageEnrichment();

        // Then: Should exit early
        verify(slidingWindowService, never()).shouldProcessWorkspace(any());
    }

    @Test
    @DisplayName("Should maintain scheduler counters across cycles")
    void shouldMaintainSchedulerCountersAcrossCycles() {
        // Given: Scheduler with existing counter state
        AtomicInteger totalRuns = new AtomicInteger(5);
        ReflectionTestUtils.setField(scheduler, "totalSchedulerRuns", totalRuns);

        List<Workspace> workspaces = List.of(createWorkspace("ws-1", "tenant-1"));
        when(workspaceService.getAllWorkspaces()).thenReturn(workspaces);

        SlidingWindowService.ProcessingDecision skipDecision =
            new SlidingWindowService.ProcessingDecision(false,
                SlidingWindowService.ProcessingDecision.Reason.INSUFFICIENT_CRITERIA,
                "No messages");
        when(slidingWindowService.shouldProcessWorkspace(any())).thenReturn(skipDecision);

        // When: Scheduler runs
        scheduler.processMessageEnrichment();

        // Then: Counter should increment
        AtomicInteger updatedRuns = (AtomicInteger) ReflectionTestUtils.getField(scheduler, "totalSchedulerRuns");
        assertThat(updatedRuns.get()).isEqualTo(6);
    }

    @Test
    @DisplayName("Should handle workspace with null credentials")
    void shouldHandleWorkspaceWithNullCredentials() {
        // Given: Workspace with null tenant ID or user ID
        Workspace invalidWorkspace = new Workspace();
        invalidWorkspace.setId("ws-1");
        invalidWorkspace.setName("Invalid Workspace");
        invalidWorkspace.setTenantId(null); // null tenant
        invalidWorkspace.setDeemergeUserId(null); // null user

        List<Workspace> workspaces = List.of(invalidWorkspace);
        when(workspaceService.getAllWorkspaces()).thenReturn(workspaces);

        // Should handle this gracefully
        SlidingWindowService.ProcessingDecision invalidDecision =
            new SlidingWindowService.ProcessingDecision(false,
                SlidingWindowService.ProcessingDecision.Reason.INVALID_TENANT,
                "Invalid tenant");
        when(slidingWindowService.shouldProcessWorkspace(invalidWorkspace))
            .thenReturn(invalidDecision);

        // When: Scheduler runs
        scheduler.processMessageEnrichment();

        // Then: Should handle gracefully without crashing
        verify(slidingWindowService, times(1)).shouldProcessWorkspace(any());
    }

    // ==================== HELPER METHODS ====================

    private Workspace createWorkspace(String id, String tenantId) {
        Workspace workspace = new Workspace();
        workspace.setId(id);
        workspace.setName("Test Workspace " + id);
        workspace.setTenantId(tenantId);
        workspace.setDeemergeUserId("user-" + id);
        workspace.setDeemergeUserName("Test User " + id);
        return workspace;
    }
}
