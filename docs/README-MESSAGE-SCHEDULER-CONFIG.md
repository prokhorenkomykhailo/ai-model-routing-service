# MessageEnrichmentScheduler Configuration

The MessageEnrichmentScheduler has been enhanced with protection against processing old/stale messages. Here are the configuration options:

## New Configuration Properties

Add these to your `application.yml` or `application.properties`:

```yaml
ai:
  enrichment:
    scheduler:
      enabled: true                    # Enable/disable the scheduler
      cron: "0 */15 * * * ?"          # Run every 15 minutes (default)
      batch-size: 1000                 # Messages per batch (default)
      default-tenant-schema: "public"  # Default schema (default)
      max-message-age-days: 30         # Don't process messages older than 30 days (NEW)
      min-recent-messages: 1           # Skip processing if fewer than 1 recent message (NEW)
```

## How It Works

### Old Message Protection

1. **Age Filtering**: Messages older than `max-message-age-days` are filtered out before processing
2. **Recent Message Check**: Workspaces with fewer than `min-recent-messages` recent messages are logged with warnings
3. **Intelligent Processing**: The SlidingWindowService already includes sophisticated logic to:
   - Only process workspaces with unprocessed messages
   - Skip workspaces processed recently (unless time threshold exceeded)
   - Mark processed messages to avoid reprocessing
   - Clean up old processed messages automatically

### Enhanced Logging

The scheduler now provides comprehensive logging:

```
📦 [BATCH-START] Processing batch #1 with 43 Redis messages for workspace: TeamSpace | 🏢 Tenant: ABC
🔍 [MESSAGE-FILTER] Batch #1 filtering messages older than 30 days
✅ [MESSAGE-FILTER] Batch #1 for workspace TeamSpace filtered: 41 recent, 2 old (>30d), 0 invalid timestamps
📊 [BATCH-STATS] Batch #1 | ⏱️ 156ms processing | 📨 43 total → 2 filtered → 41 processed | 👥 12 participants
🚀 [BATCH-PUBLISHED] Batch #1 published to ai-enrich topic | 41 messages sent for AI processing
```

### Monitoring Points

Look for these log patterns to monitor the scheduler:

- `📦 [BATCH-START]` - Batch processing begins
- `🔍 [MESSAGE-FILTER]` - Message age filtering
- `📊 [BATCH-STATS]` - Processing statistics
- `🚀 [BATCH-PUBLISHED]` - Messages sent to AI
- `✅ [WORKSPACE-SUCCESS]` - Workspace completed
- `⏭️ [WORKSPACE-SKIPPED]` - Workspace skipped (normal)
- `❌ [WORKSPACE-FAILED]` - Workspace failed (investigate)

### Default Behavior

With default settings:
- Messages older than 30 days are excluded
- At least 1 recent message is required per workspace
- SlidingWindowService provides additional intelligence:
  - Requires minimum 5 unprocessed messages OR 4 hours since last processing
  - Automatically cleans up processed messages
  - Maintains sliding window overlap for context

## Troubleshooting

### No Messages Being Processed

Check these conditions:
1. `ai.enrichment.scheduler.enabled=true`
2. Workspace has recent messages (< 30 days old)
3. SlidingWindowService criteria are met (≥5 unprocessed messages OR ≥4 hours since last processing)
4. Valid tenant IDs and workspace configuration

### Too Many Old Messages Filtered

If you need to process older messages:
```yaml
ai.enrichment.scheduler.max-message-age-days: 90  # Increase to 90 days
```

### Scheduler Running Too Frequently

Adjust the cron expression:
```yaml
ai.enrichment.scheduler.cron: "0 0 */2 * * ?"  # Every 2 hours instead of 15 minutes
```
