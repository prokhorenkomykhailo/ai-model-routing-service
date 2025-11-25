# Unified `userUniqueId` Guidance for AI Routing Service

## Why this matters
- Align all ingestion sources (Slack, Gmail, future channels) on a single cross-platform identifier
- Simplify downstream enrichment, topic summarisation, and analytics by removing Slack-ID assumptions
- Reduce fragile fallbacks and repeated display-name lookups across pipelines

## Current behaviour (baseline)
- `MessageService` prefers `IngestionUserDTO.uniqueUserId` when persisting `Message` / `SlackMessage` records (`SlackMessage.java`, `MessageService.java`)
- `TopicEnrichmentStep` builds user maps keyed by the best available ID but still falls back to Slack IDs and usernames
- `TextUtils.replaceSlackMentions` only recognises tokens shaped like `<@U123>`
- Redis lookups in `UserService` default to the legacy `getUser` (Slack ID) path; unified lookup is available but rarely used
- Gemini prompt payloads write `USER_ID` from `SlackMessage.slackUserId`, which currently aliases the resolved identifier

## Target state
1. Every ingestion path populates `IngestionUserDTO.uniqueUserId`
2. Stored records retain both the unified ID and the original provider ID
3. Mention replacement translates any provider token to the unified ID before resolving display metadata
4. All service lookups and API payloads use the dedicated unified ID field by default
5. Tests cover Slack and non-Slack events end-to-end, proving the identifier flow

## Implementation guidelines
### 1. Ingestion contracts
- Update each ingestion producer to guarantee `uniqueUserId` is set (Slack: user profile GUID, Gmail: Google sub/email, others: provider GUID)
- Reject or flag events missing the field; add telemetry so we can find outliers
- Document the contract in `IngestionUserDTO` Javadoc and relevant service READMEs

### 2. Storage schema updates
- Persist `uniqueUserId` explicitly (new column/field if needed) instead of overloading `slackUserId`
- Keep the provider-specific identifier (`slackUserId`, `gmailUserId`, etc.) for linking back to native systems
- Migrate existing Redis/docs records via script or scheduled job to backfill the unified ID

### 3. Lookup logic
- Prefer `UserService.getUserByUniqueUserId`; keep the Slack-ID method only for fallback logging
- Update `TopicEnrichmentStep.updateUserInformation` and any other lookup helpers to use the unified ID first
- Track metrics when fallbacks trigger so we can clean up remaining gaps

### 4. Mention handling
- Extend ingestion to capture a mapping from mention token → `uniqueUserId` (store in message metadata or a side map)
- Enhance `TextUtils` to resolve tokens through that mapping, then fetch the enriched display data
- Maintain graceful degradation: if the unified ID is missing, fall back to provider ID, then raw text

### 5. AI provider payloads & downstream events
- Ensure JSON/prompt builders send both `uniqueUserId` and display metadata explicitly
- Keep provider IDs in metadata only when needed for deep links or UI rendering

### 6. Testing & validation
- Add unit/integration coverage for Slack and Gmail flows confirming the unique ID is propagated and mentions render names
- Include regression tests for topic enrichment and AI prompt formatting
- Build dashboards or log searches detecting when unified IDs are absent in production traffic

## Migration checklist
- [ ] Confirm ingestion teams commit to the unified ID contract
- [ ] Land schema/DTO changes and backfill existing data
- [ ] Switch lookups and mention handling to the new helpers
- [ ] Roll out tests/telemetry and monitor for fallback usage

Keep this document aligned with implementation progress and update sections as we complete each milestone.
