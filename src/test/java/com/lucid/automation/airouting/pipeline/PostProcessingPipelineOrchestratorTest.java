package com.lucid.automation.airouting.pipeline;

import com.lucid.automation.airouting.pipeline.context.PostProcessingContext;
import com.lucid.automation.airouting.pipeline.step.PipelineStep;
import com.lucid.automation.airouting.pipeline.step.PipelineStepResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for PostProcessingPipelineOrchestrator
 */
@ExtendWith(MockitoExtension.class)
class PostProcessingPipelineOrchestratorTest {
    
    private PostProcessingPipelineOrchestrator orchestrator;
    
    @Mock
    private PipelineStep mockStep1;
    
    @Mock
    private PipelineStep mockStep2;
    
    @Mock
    private PipelineStep mockStep3;
    
    private PostProcessingContext context;
    
    @BeforeEach
    void setUp() {
        orchestrator = new PostProcessingPipelineOrchestrator();
        
        Map<String, Object> responseMap = new HashMap<>();
        responseMap.put("messageId", "test-message-123");
        responseMap.put("correlationId", "test-correlation-456");
        responseMap.put("tenantId", "test-tenant");
        
        context = new PostProcessingContext(responseMap);
        
        // Configure mock steps
        when(mockStep1.getStepName()).thenReturn("MockStep1");
        when(mockStep1.getExecutionOrder()).thenReturn(10);
        when(mockStep1.continueOnFailure()).thenReturn(false);
        
        when(mockStep2.getStepName()).thenReturn("MockStep2");
        when(mockStep2.getExecutionOrder()).thenReturn(20);
        when(mockStep2.continueOnFailure()).thenReturn(false);
        
        when(mockStep3.getStepName()).thenReturn("MockStep3");
        when(mockStep3.getExecutionOrder()).thenReturn(30);
        when(mockStep3.continueOnFailure()).thenReturn(true);
    }
    
    @Test
    void testSuccessfulPipelineExecution() {
        // Arrange
        when(mockStep1.execute(any(PostProcessingContext.class)))
            .thenReturn(PipelineStepResult.success("Step 1 completed"));
        when(mockStep2.execute(any(PostProcessingContext.class)))
            .thenReturn(PipelineStepResult.success("Step 2 completed"));
        when(mockStep3.execute(any(PostProcessingContext.class)))
            .thenReturn(PipelineStepResult.success("Step 3 completed"));
        
        List<PipelineStep> steps = List.of(mockStep1, mockStep2, mockStep3);
        
        // Act
        PostProcessingPipelineResult result = orchestrator.execute(context, steps);
        
        // Assert
        assertTrue(result.isOverallSuccess());
        assertNull(result.getErrorMessage());
        assertEquals(3, result.getExecutedStepsCount());
        assertEquals(3, result.getSuccessfulStepsCount());
        assertEquals(0, result.getFailedStepsCount());
        
        // Verify all steps were executed
        verify(mockStep1).execute(context);
        verify(mockStep2).execute(context);
        verify(mockStep3).execute(context);
    }
    
    @Test
    void testPipelineStopsOnStepFailure() {
        // Arrange
        when(mockStep1.execute(any(PostProcessingContext.class)))
            .thenReturn(PipelineStepResult.success("Step 1 completed"));
        when(mockStep2.execute(any(PostProcessingContext.class)))
            .thenReturn(PipelineStepResult.failure("Step 2 failed"));
        when(mockStep3.execute(any(PostProcessingContext.class)))
            .thenReturn(PipelineStepResult.success("Step 3 completed"));
        
        List<PipelineStep> steps = List.of(mockStep1, mockStep2, mockStep3);
        
        // Act
        PostProcessingPipelineResult result = orchestrator.execute(context, steps);
        
        // Assert
        assertFalse(result.isOverallSuccess());
        assertNotNull(result.getErrorMessage());
        assertTrue(result.getErrorMessage().contains("MockStep2"));
        assertEquals(2, result.getExecutedStepsCount());
        assertEquals(1, result.getSuccessfulStepsCount());
        assertEquals(1, result.getFailedStepsCount());
        
        // Verify step 3 was not executed
        verify(mockStep1).execute(context);
        verify(mockStep2).execute(context);
        verify(mockStep3, never()).execute(context);
    }
    
    @Test
    void testPipelineContinuesOnFailureWhenConfigured() {
        // Arrange
        when(mockStep1.execute(any(PostProcessingContext.class)))
            .thenReturn(PipelineStepResult.success("Step 1 completed"));
        when(mockStep2.execute(any(PostProcessingContext.class)))
            .thenReturn(PipelineStepResult.failure("Step 2 failed"));
        when(mockStep3.execute(any(PostProcessingContext.class)))
            .thenReturn(PipelineStepResult.success("Step 3 completed"));
        
        // Configure step 2 to continue on failure
        when(mockStep2.continueOnFailure()).thenReturn(true);
        
        List<PipelineStep> steps = List.of(mockStep1, mockStep2, mockStep3);
        
        // Act
        PostProcessingPipelineResult result = orchestrator.execute(context, steps);
        
        // Assert
        assertTrue(result.isOverallSuccess()); // Overall success because processing continued
        assertEquals(3, result.getExecutedStepsCount());
        assertEquals(2, result.getSuccessfulStepsCount());
        assertEquals(1, result.getFailedStepsCount());
        
        // Verify all steps were executed
        verify(mockStep1).execute(context);
        verify(mockStep2).execute(context);
        verify(mockStep3).execute(context);
    }
    
    @Test
    void testPipelineStepsExecutedInOrder() {
        // Arrange
        when(mockStep1.getExecutionOrder()).thenReturn(30); // Should execute last
        when(mockStep2.getExecutionOrder()).thenReturn(10); // Should execute first
        when(mockStep3.getExecutionOrder()).thenReturn(20); // Should execute second
        
        when(mockStep1.execute(any(PostProcessingContext.class)))
            .thenReturn(PipelineStepResult.success("Step 1 completed"));
        when(mockStep2.execute(any(PostProcessingContext.class)))
            .thenReturn(PipelineStepResult.success("Step 2 completed"));
        when(mockStep3.execute(any(PostProcessingContext.class)))
            .thenReturn(PipelineStepResult.success("Step 3 completed"));
        
        List<PipelineStep> steps = List.of(mockStep1, mockStep2, mockStep3);
        
        // Act
        PostProcessingPipelineResult result = orchestrator.execute(context, steps);
        
        // Assert
        assertTrue(result.isOverallSuccess());
        
        // Verify execution order using inOrder
        var inOrder = inOrder(mockStep2, mockStep3, mockStep1);
        inOrder.verify(mockStep2).execute(context); // Order 10
        inOrder.verify(mockStep3).execute(context); // Order 20
        inOrder.verify(mockStep1).execute(context); // Order 30
    }
    
    @Test
    void testPipelineSkipsRemainingSteps() {
        // Arrange
        when(mockStep1.execute(any(PostProcessingContext.class)))
            .thenReturn(PipelineStepResult.success("Step 1 completed"));
        when(mockStep2.execute(any(PostProcessingContext.class)))
            .thenReturn(PipelineStepResult.successAndSkip("Step 2 completed and skipping"));
        when(mockStep3.execute(any(PostProcessingContext.class)))
            .thenReturn(PipelineStepResult.success("Step 3 completed"));
        
        List<PipelineStep> steps = List.of(mockStep1, mockStep2, mockStep3);
        
        // Act
        PostProcessingPipelineResult result = orchestrator.execute(context, steps);
        
        // Assert
        assertTrue(result.isOverallSuccess());
        assertEquals(2, result.getExecutedStepsCount());
        assertEquals(2, result.getSuccessfulStepsCount());
        assertEquals(0, result.getFailedStepsCount());
        
        // Verify step 3 was not executed
        verify(mockStep1).execute(context);
        verify(mockStep2).execute(context);
        verify(mockStep3, never()).execute(context);
    }
    
    @Test
    void testNullContextHandling() {
        // Arrange
        List<PipelineStep> steps = List.of(mockStep1);
        
        // Act
        PostProcessingPipelineResult result = orchestrator.execute(null, steps);
        
        // Assert
        assertFalse(result.isOverallSuccess());
        assertEquals("Context is null", result.getErrorMessage());
        assertEquals(0, result.getExecutedStepsCount());
        
        // Verify no steps were executed
        verify(mockStep1, never()).execute(any());
    }
    
    @Test
    void testEmptyStepsHandling() {
        // Act
        PostProcessingPipelineResult result = orchestrator.execute(context, List.of());
        
        // Assert
        assertTrue(result.isOverallSuccess());
        assertEquals("No steps to execute", result.getErrorMessage());
        assertEquals(0, result.getExecutedStepsCount());
    }
    
    @Test
    void testUnexpectedExceptionHandling() {
        // Arrange
        when(mockStep1.execute(any(PostProcessingContext.class)))
            .thenThrow(new RuntimeException("Unexpected error"));
        
        List<PipelineStep> steps = List.of(mockStep1, mockStep2);
        
        // Act
        PostProcessingPipelineResult result = orchestrator.execute(context, steps);
        
        // Assert
        assertFalse(result.isOverallSuccess());
        assertTrue(result.getErrorMessage().contains("MockStep1"));
        assertEquals(1, result.getExecutedStepsCount());
        assertEquals(0, result.getSuccessfulStepsCount());
        assertEquals(1, result.getFailedStepsCount());
        
        // Verify step 2 was not executed
        verify(mockStep1).execute(context);
        verify(mockStep2, never()).execute(context);
        
        // Verify the step result contains the exception
        PipelineStepResult stepResult = result.getStepResult("MockStep1");
        assertNotNull(stepResult);
        assertFalse(stepResult.isSuccess());
        assertNotNull(stepResult.getException());
        assertTrue(stepResult.getException() instanceof RuntimeException);
    }
}
