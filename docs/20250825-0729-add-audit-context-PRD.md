# PRD: Add audit context fields to audit logs across services

## Project Context Overview
- Goal: Ensure audit logs include timestamp, userId, tenantId, and client IP across services.
- Reference implementation inspected in this repo:
  - [`AuditLog`](lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/audit/AuditLog.java:27)
  - [`AuditContextExtractor`](lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/audit/AuditContextExtractor.java:36)
  - [`AuditLoggingService`](lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/audit/AuditLoggingService.java:48)
- Author: vudu

## Scope
- In-scope:
  - Standardize audit payload to include UTC timestamp, userId, tenantId, client IP, correlationId.
  - Ensure per-request extraction via context extractors and populate AuditLog before emission.
  - Configure AUDIT logger routing and MDC keys for downstream systems.
- Out-of-scope:
  - Changes to CloudWatch/OpenTelemetry backends (only config recommendations).
  - Full rework of existing audit topics beyond required fields.

## Acceptance
- AC-1: All services emit audit logs containing fields: timestamp, userId, tenantId, ip (client IP) and correlationId.
  - Evidence: JSON AUDIT event includes keys (timestamp,userId,tenantId,ip,correlationId).
- AC-2: Timestamp is in UTC (ISO 8601) and set at log creation time.
- AC-3: For HTTP requests, userId/tenantId/ip are populated from request/security context; for background jobs, actor/service identity is recorded.
- AC-4: Unit tests cover extractor behavior (unknown defaults) and logging emission including MDC keys.

## What existed and what is not
- Existing artifacts (evidence):
  - Audit DTO with fields in auth service:
    - [`AuditLog`](lucid-auth-service/src/main/java/com/lucid/automation/audit/AuditLog.java:27) (timestamp,userId,tenantId,ip,correlationId,http fields)
  - AuditContextExtractor implementations:
    - [`AuditContextExtractor.getUserId()`](lucid-data-storage-service/src/main/java/com/lucid/automation/datastorage/audit/AuditContextExtractor.java:36)
    - [`AuditContextExtractor.getTenantId()`](lucid-data-storage-service/src/main/java/com/lucid/automation/datastorage/audit/AuditContextExtractor.java:60)
    - [`AuditContextExtractor.getClientIp()`](lucid-data-storage-service/src/main/java/com/lucid/automation/datastorage/audit/AuditContextExtractor.java:86)
    - [`AuditContextExtractor.getCorrelationId()`](lucid-data-storage-service/src/main/java/com/lucid/automation/datastorage/audit/AuditContextExtractor.java:102)
  - Audit emission service sets MDC and serializes JSON for AUDIT logger:
    - [`AuditLoggingService` MDC & put keys](lucid-data-storage-service/src/main/java/com/lucid/automation/datastorage/audit/AuditLoggingService.java:48)
    - [`AuditLoggingService` JSON emission](lucid-data-storage-service/src/main/java/com/lucid/automation/datastorage/audit/AuditLoggingService.java:84)
  - airouting service similar implementations:
    - [`AuditContextExtractor`](lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/audit/AuditContextExtractor.java:36)
    - [`AuditLoggingService`](lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/audit/AuditLoggingService.java:48)
- Missing / gaps:
  - No single shared AuditLog DTO in `lucid-common-dtos` identified (consider centralizing).
  - Some services may lack an AuditContextExtractor variant; repo-wide audit coverage not fully verified.

## Easy parts
- DTOs already include many required fields in auth and airouting services.
- AuditLoggingService pattern (MDC + JSON AUDIT logger) exists in multiple services.

## Hard parts / Risks
- Cross-service consistency: different services may implement slightly different DTOs or MDC keys.
- Background jobs and async flows may not have HttpServletRequest; need a policy for actor identity.
- PII and privacy: ensure IP storage complies with policy/regulations.
- Timestamp timezone inconsistency risk: some code uses LocalDateTime.now() (system-local) instead of UTC.

## Decisions (MCP sequentialthinking)
- Plan: Use existing pattern from `lucid-ai-routing-service` and `lucid-data-storage-service` as canonical examples.
- Expand: Standardize DTO, apply AuditContextExtractor usage, and configure AUDIT logger routing.
- Verify: Add unit tests to assert required fields and MDC presence before emission.
- Execute: Rollout per-service PRs to align DTOs, extractors, timestamps, and logging config.
- Reflect: Run repository-wide verification and adjust for non-HTTP flows.

## Recommended concrete steps (Execute)
1. Inventory: run codebase_search for audit classes across repos and record missing services. Owner: infra/dev lead. Estimate: 1d.
2. Standardize DTO:
   - Option A (preferred): Move `AuditLog` to `lucid-common-dtos` and reference it from services.
   - Option B: Keep per-service DTOs but ensure identical field names and JSON format.
   Owner: backend team. Estimate: 1-2d.
3. Add/confirm AuditContextExtractor in each service:
   - Ensure implementations extract userId, tenantId, client IP, correlationId with same key names.
   Owner: feature owners. Estimate: 0.5-1d per service.
4. Add or update calls to AuditLoggingService:
   - Ensure AuditLog.timestamp = now(UTC), userId, tenantId, ip populated before logAuditEvent call.
   Owner: feature owners. Estimate: 0.5-1d per endpoint.
5. Add unit tests:
   - Test extractor returns UNKNOWN defaults (see [`AuditContextExtractor.isValidId()`](lucid-data-storage-service/src/main/java/com/lucid/automation/datastorage/audit/AuditContextExtractor.java:179)).
   - Test AuditLoggingService produces AUDIT_EVENT JSON with required keys.
   Owner: engineers. Estimate: 1-2d per repo.
6. Logging configuration:
   - Ensure `AUDIT` logger routes to OpenTelemetry/CloudWatch via existing collector mapping (update logback / OTEL config).
   Owner: infra. Estimate: 0.5-1d.

## UTC timestamp recommendation
- Risk: `AuditLoggingService.logSuccess` and `logFailure` use LocalDateTime.now() which is system-local and may produce non-UTC timestamps. Evidence: [`AuditLoggingService.logSuccess` / `logFailure` uses LocalDateTime.now()`](lucid-data-storage-service/src/main/java/com/lucid/automation/datastorage/audit/AuditLoggingService.java:135).
- Audit DTO in auth serializes timestamp as UTC 'Z' pattern: [`AuditLog.timestamp` pattern](lucid-auth-service/src/main/java/com/lucid/automation/audit/AuditLog.java:27).
- Recommendation: generate timestamps in UTC using Instant/OffsetDateTime with ZoneOffset.UTC and serialize consistently across services. Add unit tests asserting timezone/format.

## Follow-ups (actionable)
- Follow-up-001: Inventory audit implementations in all services.
  owner: infra@company.example
  priority: high
  estimate: 1d
  acceptance_criteria:
    - List of services and files where audit is implemented.
- Follow-up-002: Propose DTO centralization into `lucid-common-dtos`.
  owner: backend-arch@company.example
  priority: medium
  estimate: 2d
  acceptance_criteria:
    - PR to move/alias AuditLog into `lucid-common-dtos` or justification to keep per-service DTOs.
- Follow-up-003: Add unit/integration tests for audit emission.
  owner: repo owners
  priority: high
  estimate: ongoing
  acceptance_criteria:
    - CI verifies audit JSON structure and MDC keys.
- Follow-up-004: Enforce UTC timestamps across services (replace LocalDateTime.now()).
  owner: backend-arch@company.example
  priority: high
  estimate: 1d per service
  rationale: LocalDateTime.now() is system-local; use Instant/OffsetDateTime(UTC).
  evidence:
    - [`lucid-data-storage-service/src/main/java/com/lucid/automation/datastorage/audit/AuditLoggingService.java:135-153`]
    - [`lucid-auth-service/src/main/java/com/lucid/automation/audit/AuditLog.java:27-28`]
  acceptance_criteria:
    - Audit timestamps in logs are ISO 8601 UTC and pass format check in tests.

## Consistency & Cleanup
- Consolidate AuditLog DTO:
  - Rationale: avoids drift between services. Reference: [`AuditLog`](lucid-auth-service/src/main/java/com/lucid/automation/audit/AuditLog.java:27).
- Standardize MDC key names:
  - Observed keys: `userId`, `tenantId`, `correlationId`, `clientIp` (see [`AuditLoggingService` MDC puts](lucid-data-storage-service/src/main/java/com/lucid/automation/datastorage/audit/AuditLoggingService.java:48)).
- Remove duplicated extractor logic by creating a shared `AuditContextExtractor` in `lucid-common` if appropriate.

## Privacy & Security notes
- Store client IP only if allowed by policy; consider hashing or truncation for GDPR-sensitive contexts.
- Ensure access controls on CloudWatch logs are in place.

## Reviewer checklist
- [ ] Project Context verified and references present.
- [ ] Scope correctly defined (in-scope / out-of-scope).
- [ ] Acceptance criteria measurable and testable.
- [ ] What existed section cites code evidence.
- [ ] Decisions (MCP outputs) recorded.
- [ ] Follow-ups listed with owners and priorities.
- [ ] Consistency & Cleanup actionable items present.

## Sources
- Evidence files inspected:
  - [`AuditContextExtractor`](lucid-data-storage-service/src/main/java/com/lucid/automation/datastorage/audit/AuditContextExtractor.java:36)
  - [`AuditLoggingService`](lucid-data-storage-service/src/main/java/com/lucid/automation/datastorage/audit/AuditLoggingService.java:48)
  - [`AuditLog`](lucid-auth-service/src/main/java/com/lucid/automation/audit/AuditLog.java:27)
  - [`AuditContextExtractor`](lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/audit/AuditContextExtractor.java:36)
## Repository Inventory (results of semantic search + file reads)

- Services & files with audit implementations or AOP aspects discovered:
  - [`AuditLoggingService`](lucid-data-storage-service/src/main/java/com/lucid/automation/datastorage/audit/AuditLoggingService.java:36) — structured AUDIT logger, MDC puts and JSON emission; see MDC setup and JSON emission at lines 43-89 and ensureAuditAttributes at lines 166-172.
  - [`AuditLoggingService` LocalDateTime usages](lucid-data-storage-service/src/main/java/com/lucid/automation/datastorage/audit/AuditLoggingService.java:135) — `logSuccess` / `logFailure` use `LocalDateTime.now()` (lines 135-153).
  - [`AuditContextExtractor`](lucid-data-storage-service/src/main/java/com/lucid/automation/datastorage/audit/AuditContextExtractor.java:36) — getUserId/getTenantId/getClientIp/getCorrelationId implementations and header fallbacks (see getUserId:36-55, getTenantId:60-81, getClientIp:86-97, getCorrelationId:102-138).
  - [`AuditLog` DTO`](lucid-auth-service/src/main/java/com/lucid/automation/audit/AuditLog.java:27) — DTO defines `timestamp` as `LocalDateTime` with `@JsonFormat(... 'Z')` (lines 27-28).
  - [`AuditAspect` (slack ingestion)`timestamp` usage`](lucid-slack-ingestion-service/src/main/java/com/lucid/automation/slackingestion/audit/AuditAspect.java:43) — `LocalDateTime timestamp = LocalDateTime.now();` and AuditLog build / emission (see timestamp creation at 43 and AuditLog builder at 99-114).
  - Additional services surfaced by semantic search with OpenTelemetry / logging config (candidates for inventory follow-up):
    - `lucid-ai-routing-service` (has audit aspect/logging files: see `airouting/audit/*`)
    - `lucid-gmail-ingestion-service` (OpenTelemetry config + logging utilities)
    - Other ingestion/manager services may contain audit aspects (search results returned pipeline/config artifacts).

Notes:
- Evidence above was collected by semantic search and by reading the source files listed with line citations.
- Immediate actionable observation: multiple places use `LocalDateTime.now()` to produce timestamps (example files cited above). DTO serialization expects a 'Z' suffix (UTC) but `LocalDateTime` is system-local and may not include timezone — this is a timezone mismatch risk.

## Added Follow-up: UTC timestamp enforcement (concrete)
- Follow-up-00X: Replace system-local timestamps with UTC timestamps (Instant / OffsetDateTime) and align DTO serialization.
  owner: backend-arch@company.example
  priority: high
  estimate: 1d per service (small)
  rationale: `LocalDateTime.now()` produces system-local timestamps; DTOs/serialization use UTC 'Z' pattern (see mismatch).
  evidence:
    - [`lucid-data-storage-service/src/main/java/com/lucid/automation/datastorage/audit/AuditLoggingService.java:135-153`](lucid-data-storage-service/src/main/java/com/lucid/automation/datastorage/audit/AuditLoggingService.java:135)
    - [`lucid-auth-service/src/main/java/com/lucid/automation/audit/AuditLog.java:27-28`](lucid-auth-service/src/main/java/com/lucid/automation/audit/AuditLog.java:27)
    - [`lucid-slack-ingestion-service/src/main/java/com/lucid/automation/slackingestion/audit/AuditAspect.java:43`](lucid-slack-ingestion-service/src/main/java/com/lucid/automation/slackingestion/audit/AuditAspect.java:43)
  acceptance_criteria:
    - All occurrences of `LocalDateTime.now()` in audit paths are removed or replaced with UTC-anchored time (e.g., `Instant.now()` or `OffsetDateTime.now(ZoneOffset.UTC)`).
    - `AuditLog.timestamp` DTO is updated (or marshalled) to produce ISO-8601 UTC with 'Z' and tests validate format.
    - Unit tests added to assert produced audit JSON timestamp matches ISO-8601 UTC with 'Z'.

## Small implementation guidance (for engineers; record-only — do not change code yet)
- Preferred timestamp type: use `java.time.Instant` for storage and transport; if human-readable timezone is needed use `OffsetDateTime` with `ZoneOffset.UTC`.
- DTO change option: change `AuditLog.timestamp` type from `LocalDateTime` → `OffsetDateTime`/`Instant` and update `@JsonFormat` or use `com.fasterxml.jackson.datatype:jsr310` module for consistent serialization.
- Where changing DTO is disruptive, alternative is to keep `LocalDateTime` but populate it from `OffsetDateTime` via `.toLocalDateTime()` while still ensuring serialization includes 'Z' — less preferred (risky).

## Next steps performed / suggested
- I ran a repo semantic search and read representative files in data-storage, auth, and slack-ingestion to validate timestamp mismatch and extractor patterns.
- Suggested immediate next action: run a targeted codebase_search for `LocalDateTime.now()` occurrences restricted to audit-related directories to produce a full replace list (I can run this and append results to the PRD if you confirm).
