# Tenant Onboarding Troubleshooting Guide (Post-refactor)

## Project Context — summary
- After a recent refactor, tenant onboarding fails with `InternalServerException: Failed to create tenant schema` (stacktrace provided by team).
- Failure surface: [`lucid-auth-service/src/main/java/com/lucid/automation/service/TenantOnboardingService.java:79`](lucid-auth-service/src/main/java/com/lucid/automation/service/TenantOnboardingService.java:79)
- Caller flow includes: [`TenantService.createTenant`](lucid-auth-service/src/main/java/com/lucid/automation/service/TenantService.java:169) -> `TenantOnboardingService.onboardTenant` -> webhook handling (`ClerkWebhookService` / `ClerkWebhookController`).

## Author
vudu

## Scope (in-scope / out-of-scope)
- In-scope: triage steps, reproduction checklist, log collection, database checks, common causes introduced by refactor.
- Out-of-scope: source code edits. Any code fixes must be proposed as PRs and documented in follow-ups.

## Acceptance — measurable success criteria
1. The failure is reproducible locally with DEBUG logs captured.
2. Root cause is identified and documented (one of: DB permissions, missing migrations, misconfiguration, transaction issues, packaging).
3. A remediation plan is proposed and validated (onboarding succeeds in a local/dev environment or CI).

## What existed and what is not
- Existing artifacts:
  - Stacktrace indicates failure at `TenantOnboardingService.onboardTenant` line 79.
  - Caller references: [`lucid-auth-service/src/main/java/com/lucid/automation/service/TenantService.java:169`](lucid-auth-service/src/main/java/com/lucid/automation/service/TenantService.java:169), and webhook handler frames such as [`lucid-auth-service/src/main/java/com/lucid/automation/service/ClerkWebhookService.java:288`](lucid-auth-service/src/main/java/com/lucid/automation/service/ClerkWebhookService.java:288).
- Missing / gaps:
  - No formal post-refactor migration & packaging checklist recorded in repository.
  - No centralized troubleshooting runbook for schema creation failures (this document fills that gap).

## Reproduction steps (developer)
1. Re-run the same webhook payload that triggered the error (sanitize secrets before sharing).
2. Start the service locally with DEBUG logging:
   - set `logging.level.com.lucid.automation=DEBUG` or pass JVM arg `-Dlogging.level.com.lucid.automation=DEBUG`.
3. Invoke the webhook / tenant creation flow and capture application logs (stdout and any file logs).
4. Collect DB server logs (Postgres/MySQL) and any query logs for the same timestamp.

## Logs & evidence to collect
- Full Java stacktrace (complete).
- Application logs +/- 30s around the failure (include thread and MDC).
- SQL statements executed and exact DB error messages.
- Migration engine state (Flyway / Liquibase history).
- Deployment artifact metadata: jar name, build commit hash, and packaging timestamp.

## Focused checks (likely causes after refactor)
1. Database connectivity or credentials changed during refactor:
   - Validate `spring.datasource.*` values and secret interpolation.
2. Missing migrations or DDL files not packaged:
   - Confirm migration history and that SQL resources are present in the built jar.
3. DB user privileges:
   - Schema creation may require CREATE SCHEMA or elevated privileges.
4. Transactional behavior:
   - Schema creation executed inside a transaction may be rolled back; check propagation and rollback settings.
5. Classpath/resource relocation:
   - SQL templates, DDL, or classpath-resident scripts moved in refactor, causing resource lookup failures.
6. Profile/config merges:
   - Ensure the correct Spring profile is active and config merges produce expected values.

## Quick diagnostic SQL (DB admin)
- Validate current user privileges (DB-specific).
- If Flyway used, inspect migration table:
  - `SELECT * FROM flyway_schema_history ORDER BY installed_on DESC;`

## Recommended immediate remediation steps
1. Reproduce failure locally and attach logs to an issue/PR.
2. If privileges are insufficient, either grant required DB privileges or run the schema creation as an ops migration step.
3. If migration/DDL files are missing from jar, fix packaging (resource paths) and rebuild.
4. If transactional rollback is hiding DDL results, move schema creation out of the main transaction or use PROPAGATION_REQUIRES_NEW.

## How to debug `TenantOnboardingService.onboardTenant` quickly
- Add temporary DEBUG logs around the failing line to print inputs, SQL, and caught exceptions. Reference: [`lucid-auth-service/src/main/java/com/lucid/automation/service/TenantOnboardingService.java:79`](lucid-auth-service/src/main/java/com/lucid/automation/service/TenantOnboardingService.java:79)
- Replay failing SQL manually against the DB to get the precise DB error.

## Consistency & Cleanup
- Add a post-refactor checklist to the repo documenting packaging/migration expectations.
- Centralize DB bootstrap scripts and document required DB roles and permissions.
- Add CI integration test that runs tenant onboarding against a disposable DB (Docker) to catch regressions.

## Follow-ups
- Follow-up-001: Reproduce failure and attach logs (owner: oncall-dev, priority: high).
  acceptance_criteria:
    - Full logs and sanitized webhook payload attached to issue/PR.
- Follow-up-002: Verify DB user privileges and migration presence (owner: db-admin, priority: high).
- Follow-up-003: Add CI integration test for onboarding (owner: dev, priority: medium).
- Follow-up-004: Add post-refactor packaging & migration checklist to docs/ (owner: dev, priority: medium).

## Reviewer checklist
- [ ] Reproduction logs attached
- [ ] DB migration history verified
- [ ] Privileges and config validated
- [ ] Remediation plan proposed

## References
- Stacktrace excerpt provided in incident report.
- Code references:
  - [`lucid-auth-service/src/main/java/com/lucid/automation/service/TenantOnboardingService.java:79`](lucid-auth-service/src/main/java/com/lucid/automation/service/TenantOnboardingService.java:79)
  - [`lucid-auth-service/src/main/java/com/lucid/automation/service/TenantService.java:169`](lucid-auth-service/src/main/java/com/lucid/automation/service/TenantService.java:169)

End.