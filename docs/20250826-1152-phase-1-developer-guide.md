# Phase 1 Developer Guide — AI Routing Service

## Project Context
- Summary: implement Phase 1 improvements from [`lucid-ai-routing-service/docs/ARCHITECTURE_ANALYSIS.md`](lucid-ai-routing-service/docs/ARCHITECTURE_ANALYSIS.md:1).
- Goal: fix token availability logic, runtime health checks, provider failover and improve observability for AI routing.
- Author: vudu

## Scope
### In-scope
- Fix and harden TokenAvailabilityService and related client integrations.
- Add runtime health checks and scheduled health checker.
- Implement provider failover and provider-router improvements.
- Add metrics, tracing, and alerting hooks as described in architecture doc.
- Unit and integration tests for new behaviors.

### Out-of-scope
- Rewriting provider clients (only adapters/routers changes).
- Infra changes (Kafka topics, partitions, ACLs) — record as follow-ups.

## Acceptance
- All unit tests for TokenAvailabilityService pass.
- Health endpoint reports provider health and aggregated token availability.
- Provider failover triggers to fallback provider within configured timeout in simulated failure tests.
- Metrics exposed: provider_up_count, provider_failover_count, token_available_count, token_unavailable_count, routing_latency_ms.
- Tracing spans present for provider selection and external call with correlationId.

## What existed and what is not
- Existing architecture doc: [`lucid-ai-routing-service/docs/ARCHITECTURE_ANALYSIS.md`](lucid-ai-routing-service/docs/ARCHITECTURE_ANALYSIS.md:1-552).
- AI message consumer scaffold: [`lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/consumer/AIMessageConsumer.java`](lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/consumer/AIMessageConsumer.java:76-93) — contains provider selection snippet.
- Common DTOs and provider id fields: [`lucid-common-dtos/src/main/java/com/lucid/automation/common/dto/ai/TextQueryResponseDTO.java`](lucid-common-dtos/src/main/java/com/lucid/automation/common/dto/ai/TextQueryResponseDTO.java:30-31).
- Auth token / security filters in routing service: [`lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/security/JwtAuthenticationFilter.java`](lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/security/JwtAuthenticationFilter.java:88-105).
- No dedicated TokenAvailabilityService file found by earlier searches; implementation points to be added or located by developers.

## Recommended Implementation Steps
1. Inventory current provider clients and configuration
   - Locate provider factory / registry (search for AIProviderFactory and AIProvider interfaces).
   - Confirm provider config entries in application.yml.
   - References: provider usage snippet in [`AIMessageConsumer`](lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/consumer/AIMessageConsumer.java:76-93).
2. Implement/repair TokenAvailabilityService
   - Responsibilities:
     - Maintain token counts/quotas per provider and tenant.
     - Expose an API to check availability synchronously and an async scheduler to refresh tokens.
   - Tests:
     - Unit tests that simulate token exhaustion and replenishment.
3. Add scheduled health checker
   - Implement a scheduled job that pings each provider health endpoint and updates an internal status map.
   - Expose provider health via Actuator health indicator.
   - Reference example patterns in [`lucid-ai-routing-service/docs/ARCHITECTURE_ANALYSIS.md`](lucid-ai-routing-service/docs/ARCHITECTURE_ANALYSIS.md:200-260).
4. Provider router & failover
   - Add provider selection logic:
     - honor preferredProvider if present.
     - if preferredProvider unavailable, select next available provider using priority list.
     - on provider call failure, retry with exponential backoff then failover to next provider.
   - Ensure per-request correlationId is propagated (see kafka producer `correlationId` usage in deletion events for pattern [`lucid-auth-service/src/main/java/com/lucid/automation/service/UserDeletionEventProducer.java`](lucid-auth-service/src/main/java/com/lucid/automation/service/UserDeletionEventProducer.java:1-175) for header patterns).
5. Observability & metrics
   - Add Micrometer metrics: provider_up_count, provider_failover_count, token_available_count, routing_latency_ms.
   - Add OpenTelemetry spans around provider selection and external calls (`OpenTelemetryConfig` pattern available in other services like [`lucid-auth-service/src/main/java/com/lucid/automation/logging/OpenTelemetryConfig.java`](lucid-auth-service/src/main/java/com/lucid/automation/logging/OpenTelemetryConfig.java:1)).
6. Health endpoints & Actuator
   - Wire provider health into Spring Boot Actuator (`HealthIndicator`) and ensure readiness/liveness semantics.
7. Tests
   - Unit tests for TokenAvailabilityService and provider-router.
   - Integration tests that mock provider endpoints and assert failover behavior.

## Files to Inspect / Modify (candidate locations)
- [`lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/consumer/AIMessageConsumer.java`](lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/consumer/AIMessageConsumer.java:1-120)
- Provider factory / interfaces (search for `AIProviderFactory`, `AIProvider`) — example usage: [`AIMessageConsumer.java`](lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/consumer/AIMessageConsumer.java:76-93).
- Security & token handling: [`lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/security/JwtAuthenticationFilter.java`](lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/security/JwtAuthenticationFilter.java:88-105).
- Config: [`lucid-ai-routing-service/src/main/resources/application.yml`](lucid-ai-routing-service/src/main/resources/application.yml:1).

## Decisions (MCP Sequential Thinking)
- Plan: prioritize TokenAvailabilityService, health checks, provider failover as Phase 1 (from architecture doc) — rationale: these reduce downtime and prevent request failures.
- Expand: add scheduled health checker, metrics, and failover; prefer non-breaking additions (adapters, indicators).
- Verify: unit and integration tests required; add canary test to simulate provider outage.
- Execute: implement TokenAvailabilityService + router, add HealthIndicator, add metrics, run tests.
- Reflect: measure provider_failover_count and routing_latency_ms for 2 weeks; adjust thresholds.
- Source evidence: architecture doc [`ARCHITECTURE_ANALYSIS.md`](lucid-ai-routing-service/docs/ARCHITECTURE_ANALYSIS.md:1-552) and consumer snippet [`AIMessageConsumer.java`](lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/consumer/AIMessageConsumer.java:76-93).

## Easy parts and Hard parts
- Easy:
  - Adding metrics and tracing (existing OpenTelemetry config pattern).
  - Implementing HealthIndicator wrappers for providers.
- Hard:
  - Designing TokenAvailabilityService with correct concurrency and eventual consistency.
  - Ensuring failover does not violate per-tenant rate limits or contractual provider quotas.
  - Cross-service coordination for token refresh (if keys are shared).

## Consistency & Cleanup
- Reuse provider interfaces where present; avoid duplicating DTOs. Reference shared DTO: [`lucid-common-dtos/src/main/java/com/lucid/automation/common/dto/ai/TextQueryResponseDTO.java`](lucid-common-dtos/src/main/java/com/lucid/automation/common/dto/ai/TextQueryResponseDTO.java:30-31).
- Remove commented-out import placeholders in [`AIMessageConsumer.java`](lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/consumer/AIMessageConsumer.java:5-6).
- Add unit tests and remove any dead code discovered during implementation.

## Follow-ups
- Follow-up-ARCH-001: Confirm provider priority list and SLA for failover (owner: platform/product) — priority: high.
- Follow-up-DEV-001: Implement TokenAvailabilityService prototype and submit PR (owner: ai-routing team) — priority: high.
- Follow-up-OPS-001: Add monitoring dashboards and alerts for provider_failover_count and routing_latency_ms (owner: infra/observability) — priority: medium.
- Follow-up-SEC-001: Validate token/key storage and rotation for provider clients (owner: security) — priority: medium.

## Execution (recommended concrete steps)
- Step 1: Search repo for `AIProvider`, `AIProviderFactory`, and existing provider clients; create tech design doc (1 day).
- Step 2: Prototype TokenAvailabilityService and HealthIndicator (2-3 days).
- Step 3: Implement provider-router failover with tests (3-4 days).
- Step 4: Add metrics/tracing and deploy to staging; run canary scenarios (2 days).

## Reviewer checklist
- [ ] Project Context verified against [`ARCHITECTURE_ANALYSIS.md`](lucid-ai-routing-service/docs/ARCHITECTURE_ANALYSIS.md:1-552).
- [ ] Scope and Acceptance criteria present.
- [ ] Files to modify and tests identified.
- [ ] Decisions (MCP) recorded.
- [ ] Follow-ups listed with owners and priorities.

## Sources
- [`lucid-ai-routing-service/docs/ARCHITECTURE_ANALYSIS.md`](lucid-ai-routing-service/docs/ARCHITECTURE_ANALYSIS.md:1-552)
- [`lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/consumer/AIMessageConsumer.java`](lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/consumer/AIMessageConsumer.java:76-93)
- [`lucid-common-dtos/src/main/java/com/lucid/automation/common/dto/ai/TextQueryResponseDTO.java`](lucid-common-dtos/src/main/java/com/lucid/automation/common/dto/ai/TextQueryResponseDTO.java:30-31)
- [`lucid-auth-service/src/main/java/com/lucid/automation/service/UserDeletionEventProducer.java`](lucid-auth-service/src/main/java/com/lucid/automation/service/UserDeletionEventProducer.java:1-175)

-- End of guide