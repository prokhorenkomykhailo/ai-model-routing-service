package com.lucid.automation.airouting.scheduler;

import com.lucid.automation.airouting.model.Message;
import com.lucid.automation.airouting.repository.MessageRepository;
import com.lucid.automation.airouting.service.MessageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Scheduler for automatically cleaning up soft-deleted messages that have exceeded their retention period.
 * <p>
 * This scheduler runs daily (default: 3 AM) to permanently delete messages that were soft-deleted
 * more than 30 days ago. Soft-deleted messages are marked with isDeleted=true and have a
 * retentionExpiry timestamp calculated at deletion time.
 * <p>
 * <b>Cleanup Process:</b>
 * <ol>
 *   <li>Query Redis for messages where isDeleted=true AND retentionExpiry < currentTime</li>
 *   <li>Process expired messages in batches (default: 100)</li>
 *   <li>Permanently delete each expired message using messageService.hardDeleteExpired()</li>
 *   <li>Log comprehensive statistics (deleted count, errors, duration)</li>
 * </ol>
 * <p>
 * <b>Configuration:</b>
 * <pre>
 * message:
 *   retention:
 *     cleanup:
 *       enabled: true                 # Enable/disable scheduler
 *       cron: "0 0 3 * * ?"          # Daily at 3 AM
 *       batch-size: 100              # Process 100 messages per batch
 * </pre>
 * <p>
 * <b>Logging Examples:</b>
 * <ul>
 *   <li>✅ Success: "Successfully deleted 45 expired messages in 1523ms"</li>
 *   <li>📭 No work: "No expired messages found for cleanup"</li>
 *   <li>❌ Error: "Failed to delete message msg123: Redis connection timeout"</li>
 * </ul>
 *
 * @author vudu
 * @see MessageService#hardDeleteExpired(String)
 * @see Message#getRetentionExpiry()
 * @since 1.0.0
 */
@Component
@ConditionalOnProperty(value = "message.retention.cleanup.enabled", havingValue = "true", matchIfMissing = false)
@RequiredArgsConstructor
@Slf4j
public class MessageRetentionCleanupScheduler {

    private final MessageRepository messageRepository;
    private final MessageService messageService;

    @Value("${message.retention.cleanup.batch-size:100}")
    private int batchSize;

    @Value("${message.retention.cleanup.job-name:message-retention-cleanup}")
    private String jobName;

    // Scheduler-level statistics
    private final AtomicInteger totalSchedulerRuns = new AtomicInteger(0);
    private final AtomicInteger totalMessagesDeleted = new AtomicInteger(0);
    private final AtomicInteger totalErrors = new AtomicInteger(0);

    @PostConstruct
    public void postConstruct() {
        log.info("📅 === MessageRetentionCleanupScheduler @PostConstruct Called ===");
        log.info("⚙️ [CONFIG] Retention Cleanup Configuration:");
        log.info("⚙️ [CONFIG]   - Job Name: {}", jobName);
        log.info("⚙️ [CONFIG]   - Batch Size: {}", batchSize);
        log.info("⚙️ [CONFIG]   - Schedule: Daily at 3 AM (0 0 3 * * ?)");
        log.info("⚙️ [CONFIG]   - Retention Period: 30 days (configured in MessageService.softDeleteById)");
        log.info("✅ MessageRetentionCleanupScheduler bean successfully created and initialized");
        log.info("⏰ Next scheduled execution will occur at 3 AM according to cron expression");
        log.info("📅 === MessageRetentionCleanupScheduler @PostConstruct Completed ===");
    }

    /**
     * Scheduled job to permanently delete expired soft-deleted messages.
     * Runs daily at 3 AM (configurable via cron expression).
     * <p>
     * <b>Execution Flow:</b>
     * <ol>
     *   <li>Query Redis for expired messages (isDeleted=true, retentionExpiry < now)</li>
     *   <li>Process in batches to avoid memory issues</li>
     *   <li>Call hardDeleteExpired() for each message</li>
     *   <li>Track statistics and log results</li>
     * </ol>
     */
    @Scheduled(cron = "${message.retention.cleanup.cron:0 0 3 * * ?}")
    public void runRetentionCleanup() {
        long startTime = System.currentTimeMillis();
        int runNumber = totalSchedulerRuns.incrementAndGet();
        String startTimeFormatted = DateTimeFormatter.ISO_LOCAL_TIME.format(LocalDateTime.now());

        log.info("🚀 === RETENTION CLEANUP EXECUTION #{} STARTED === ⏰ {}", runNumber, startTimeFormatted);
        log.info("📊 [SCHEDULER-STATS] Historical totals - Runs: {} | Deleted: {} | Errors: {}",
                runNumber, totalMessagesDeleted.get(), totalErrors.get());

        try {
            // Find all expired messages (isDeleted=true AND retentionExpiry < currentTime)
            long currentTime = System.currentTimeMillis();
            
            // Fetch expired messages in pages to avoid loading too many at once
            PageRequest pageRequest = PageRequest.of(0, batchSize);
            List<Message> expiredMessages = messageRepository.findByIsDeletedAndRetentionExpiryLessThan(true, currentTime, pageRequest);

            if (expiredMessages == null || expiredMessages.isEmpty()) {
                log.info("📭 No expired messages found for cleanup. All soft-deleted messages are still within retention period.");
                long totalTime = System.currentTimeMillis() - startTime;
                log.info("✅ [CLEANUP-COMPLETE] Execution #{} completed in {}ms with no work to do", runNumber, totalTime);
                return;
            }

            log.info("🗑️ Found {} expired messages to permanently delete", expiredMessages.size());

            // Process messages and track statistics
            int successCount = 0;
            int errorCount = 0;

            for (Message message : expiredMessages) {
                try {
                    String messageId = message.getId();
                    messageService.hardDeleteExpired(messageId);
                    successCount++;
                    totalMessagesDeleted.incrementAndGet();

                    if (log.isDebugEnabled()) {
                        log.debug("✅ [HARD-DELETE] Successfully deleted expired message: {} | Deleted by: {} | Reason: {} | Expired: {}ms ago",
                                messageId, message.getDeletedBy(), message.getDeletionReason(),
                                currentTime - message.getRetentionExpiry());
                    }

                } catch (Exception e) {
                    errorCount++;
                    totalErrors.incrementAndGet();
                    log.error("❌ [HARD-DELETE] Failed to delete expired message {}: {}",
                            message.getId(), e.getMessage(), e);
                }
            }

            long totalTime = System.currentTimeMillis() - startTime;
            String endTimeFormatted = DateTimeFormatter.ISO_LOCAL_TIME.format(LocalDateTime.now());

            log.info("✅ [CLEANUP-SUCCESS] Execution #{} completed in {}ms at {} | ✅ {} deleted | ❌ {} errors | 📊 Total: {} found",
                    runNumber, totalTime, endTimeFormatted, successCount, errorCount, expiredMessages.size());
            log.info("📊 [GLOBAL-STATS] Overall totals - Runs: {} | Deleted: {} | Errors: {}",
                    runNumber, totalMessagesDeleted.get(), totalErrors.get());

        } catch (Exception e) {
            long totalTime = System.currentTimeMillis() - startTime;
            log.error("🚨 [SCHEDULER-ERROR] Critical error during retention cleanup execution #{} after {}ms: {}",
                    runNumber, totalTime, e.getMessage(), e);
        }

        log.info("🏁 === RETENTION CLEANUP EXECUTION #{} COMPLETED ===", runNumber);
    }
}
