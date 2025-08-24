package com.lucid.automation.airouting.service;

import com.lucid.automation.common.dto.messaging.ProgressJobStatusDTO;
import com.lucid.automation.airouting.config.KafkaTopicProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class JobServiceTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Mock
    private KafkaTopicProperties kafkaTopicProperties;

    @Mock
    private CompletableFuture<SendResult<String, Object>> future;

    private JobService jobService;

    @BeforeEach
    void setUp() {
        when(kafkaTopicProperties.getIngestionProgress()).thenReturn("ingestion-progress");
        jobService = new JobService(kafkaTemplate, kafkaTopicProperties);
    }

    @Test
    void testPublishJobStarted() {
        // Given
        String jobId = "job-123";
        String tenantId = "tenant-xyz";
        String tenantSchema = "tenant_xyz_schema";

        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(future);

        // When
        jobService.publishJobStarted(jobId, null, tenantId, tenantSchema);

        // Then
        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ProgressJobStatusDTO> messageCaptor = ArgumentCaptor.forClass(ProgressJobStatusDTO.class);

        verify(kafkaTemplate).send(topicCaptor.capture(), keyCaptor.capture(), messageCaptor.capture());

        assertEquals("ingestion-progress", topicCaptor.getValue());
        assertEquals(tenantId, keyCaptor.getValue());

        ProgressJobStatusDTO message = messageCaptor.getValue();
        assertEquals(jobId, message.getJobId());
        assertEquals("CREATION", message.getType()); // Verify AI routing jobs use CREATION type
        assertEquals(tenantId, message.getTenantId());
        assertEquals(tenantSchema, message.getTenantSchema());
        assertEquals("starting", message.getStage());
        assertEquals(0, message.getPercent());
        assertNotNull(message.getTimestamp());
    }

    @Test
    void testPublishEnrichmentProgress() {
        // Given
        String jobId = "job-123";
        String tenantId = "tenant-xyz";
        String tenantSchema = "tenant_xyz_schema";
        Integer percent = 60;
        Integer timeLeftEta = 90;

        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(future);

        // When
        jobService.publishEnrichmentProgress(jobId, null, tenantId, tenantSchema, percent, timeLeftEta);

        // Then
        ArgumentCaptor<ProgressJobStatusDTO> messageCaptor = ArgumentCaptor.forClass(ProgressJobStatusDTO.class);
        verify(kafkaTemplate).send(anyString(), anyString(), messageCaptor.capture());

        ProgressJobStatusDTO message = messageCaptor.getValue();
        assertEquals(jobId, message.getJobId());
        assertEquals(tenantId, message.getTenantId());
        assertEquals(tenantSchema, message.getTenantSchema());
        assertEquals("enriching", message.getStage());
        assertEquals(percent, message.getPercent());
        assertEquals(timeLeftEta, message.getTimeLeftEta());
        assertNotNull(message.getTimestamp());
    }

    @Test
    void testPublishCategorizationProgress() {
        // Given
        String jobId = "job-123";
        String tenantId = "tenant-xyz";
        String tenantSchema = "tenant_xyz_schema";
        Integer percent = 25;
        Integer timeLeftEta = 180;

        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(future);

        // When
        jobService.publishCategorizationProgress(jobId, null, tenantId, tenantSchema, percent, timeLeftEta);

        // Then
        ArgumentCaptor<ProgressJobStatusDTO> messageCaptor = ArgumentCaptor.forClass(ProgressJobStatusDTO.class);
        verify(kafkaTemplate).send(anyString(), anyString(), messageCaptor.capture());

        ProgressJobStatusDTO message = messageCaptor.getValue();
        assertEquals(jobId, message.getJobId());
        assertEquals(tenantId, message.getTenantId());
        assertEquals(tenantSchema, message.getTenantSchema());
        assertEquals("categorizing", message.getStage());
        assertEquals(percent, message.getPercent());
        assertEquals(timeLeftEta, message.getTimeLeftEta());
        assertNotNull(message.getTimestamp());
    }

    @Test
    void testPublishTopicCreationProgressWithTopics() {
        // Given
        String jobId = "job-123";
        String tenantId = "tenant-xyz";
        String tenantSchema = "tenant_xyz_schema";
        Integer percent = 80;

        ProgressJobStatusDTO.TopicProgress topicProgress1 = ProgressJobStatusDTO.TopicProgress.builder()
                .id("topic-1")
                .name("Sales Reports")
                .status("completed")
                .percent(100)
                .build();

        ProgressJobStatusDTO.TopicProgress topicProgress2 = ProgressJobStatusDTO.TopicProgress.builder()
                .id("topic-2")
                .name("Customer Feedback")
                .status("processing")
                .percent(60)
                .build();

        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(future);

        // When
        jobService.publishTopicCreationProgress(jobId, null, tenantId, tenantSchema, percent,
                java.util.Arrays.asList(topicProgress1, topicProgress2));

        // Then
        ArgumentCaptor<ProgressJobStatusDTO> messageCaptor = ArgumentCaptor.forClass(ProgressJobStatusDTO.class);
        verify(kafkaTemplate).send(anyString(), anyString(), messageCaptor.capture());

        ProgressJobStatusDTO message = messageCaptor.getValue();
        assertEquals(jobId, message.getJobId());
        assertEquals("CREATION", message.getType()); // Verify topic creation gets CREATION type
        assertEquals(tenantId, message.getTenantId());
        assertEquals(tenantSchema, message.getTenantSchema());
        assertEquals("topic_creation", message.getStage());
        assertEquals(percent, message.getPercent());
        assertNotNull(message.getTopics());
        assertEquals(2, message.getTopics().size());

        // Check first topic
        assertEquals("topic-1", message.getTopics().get(0).getId());
        assertEquals("Sales Reports", message.getTopics().get(0).getName());
        assertEquals("completed", message.getTopics().get(0).getStatus());
        assertEquals(100, message.getTopics().get(0).getPercent());

        // Check second topic
        assertEquals("topic-2", message.getTopics().get(1).getId());
        assertEquals("Customer Feedback", message.getTopics().get(1).getName());
        assertEquals("processing", message.getTopics().get(1).getStatus());
        assertEquals(60, message.getTopics().get(1).getPercent());

        assertNotNull(message.getTimestamp());
    }

    @Test
    void testPublishJobCompleted() {
        // Given
        String jobId = "job-123";
        String tenantId = "tenant-xyz";
        String tenantSchema = "tenant_xyz_schema";

        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(future);

        // When
        jobService.publishJobCompleted(jobId, null, tenantId, tenantSchema);

        // Then
        ArgumentCaptor<ProgressJobStatusDTO> messageCaptor = ArgumentCaptor.forClass(ProgressJobStatusDTO.class);
        verify(kafkaTemplate).send(anyString(), anyString(), messageCaptor.capture());

        ProgressJobStatusDTO message = messageCaptor.getValue();
        assertEquals(jobId, message.getJobId());
        assertEquals(tenantId, message.getTenantId());
        assertEquals(tenantSchema, message.getTenantSchema());
        assertEquals("completed", message.getStage());
        assertEquals(100, message.getPercent());
        assertNotNull(message.getTimestamp());
    }

    @Test
    void testPublishSummarizationProgress() {
        // Given
        String jobId = "job-456";
        String tenantId = "tenant-abc";
        String tenantSchema = "tenant_abc_schema";
        Integer percent = 35;
        Integer timeLeftEta = 240;

        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(future);

        // When
        jobService.publishSummarizationProgress(jobId, null, tenantId, tenantSchema, percent, timeLeftEta);

        // Then
        ArgumentCaptor<ProgressJobStatusDTO> messageCaptor = ArgumentCaptor.forClass(ProgressJobStatusDTO.class);
        verify(kafkaTemplate).send(anyString(), anyString(), messageCaptor.capture());

        ProgressJobStatusDTO message = messageCaptor.getValue();
        assertEquals(jobId, message.getJobId());
        assertEquals(tenantId, message.getTenantId());
        assertEquals(tenantSchema, message.getTenantSchema());
        assertEquals("summarizing", message.getStage());
        assertEquals(percent, message.getPercent());
        assertEquals(timeLeftEta, message.getTimeLeftEta());
        assertNotNull(message.getTimestamp());
    }
}
