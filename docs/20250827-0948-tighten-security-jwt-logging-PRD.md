# PRD: Tighten SecurityConfig and Add Enhanced JwtAuthenticationFilter Logging

## Project Context Overview
- Issue: runtime errors originating from the JWT authentication filter were observed in logs:
  - "2025-08-27T09:41:12.630Z ERROR '' :   [c.l.a.s.JwtAuthenticationFilter:] {thread.name="http-nio-8088-exec-4"}"
  - "2025-08-27T09:41:12.630Z ERROR '' :   [c.l.a.e.GlobalExceptionHandler:] {thread.name="http-nio-8088-exec-4"}"
- Investigation found:
  - The filter implementation is at [`lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/security/JwtAuthenticationFilter.java`](lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/security/JwtAuthenticationFilter.java:1). Key regions: filter entry and public-path checks (≈ lines 53–66), JWT parsing & exception handlers (≈ lines 76–131), handleUnauthorized (≈ lines 184–188).
  - The security configuration currently contains an overly-broad permitAll matcher at [`lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/security/SecurityConfig.java`](lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/security/SecurityConfig.java:1) (see `.requestMatchers("/**").permitAll()` around line 31).
  - No local project `GlobalExceptionHandler` (@ControllerAdvice) found by repo search; runtime logs reference one — likely a deployed-artifact/classpath mismatch or handler in a different module/dependency.

## Author
- vudu

## Scope
In-scope:
- Produce a small, precise PR specification containing:
  - Recommended SecurityConfig matcher tightening.
  - Enhanced JwtAuthenticationFilter logging recommendations and exact diff to apply.
  - Acceptance criteria, decisions (MCP), follow-ups, and consistency/cleanup notes.
- Do NOT change source code in this PRD. This document provides the patch for maintainers.

Out-of-scope:
- Applying the code changes directly (x51Cto restricted to docs).
- CI runs, build/test execution, or deployment steps (ops/dev to perform).

## Acceptance
- Proposed diffs can be applied as-is with minimal compile changes.
- After applying diffs and restarting, JWT-related WARN/ERROR logs should include richer context (request method, path, header presence) and show stack traces under DEBUG.
- No broad permitAll should remain; protected endpoints (e.g., `/api/tenants/**`) must require authentication.
- Tests demonstrating rejected malformed/expired JWTs for protected endpoints pass (manual or CI tests).

## What existed and what is not
- Existing artifacts:
  - [`lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/security/JwtAuthenticationFilter.java`](lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/security/JwtAuthenticationFilter.java:1) — custom OncePerRequestFilter read (lines 1–189).
  - [`lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/security/SecurityConfig.java`](lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/security/SecurityConfig.java:1) — SecurityFilterChain bean with `.requestMatchers("/**").permitAll()` (lines 1–51).
- Missing / gaps:
  - No repository-local `GlobalExceptionHandler` (@ControllerAdvice) found by search. Runtime logs reference `GlobalExceptionHandler` — confirm deployed artifact vs workspace.
  - No unit/integration tests covering JWT error paths and configuration mismatch found (add tests after changes).

## Easy parts and Hard parts
- Easy:
  - Add debug logging lines to the filter — low-risk code change.
  - Adjust SecurityConfig to remove global permitAll and explicitly list public routes — low complexity.
- Hard:
  - Verifying deployed artifact/classpath matches workspace (requires ops).
  - Ensuring no unintended endpoints become protected by tightening matchers (requires integration or smoke tests).
  - If GlobalExceptionHandler is in another module/dependency, reconciliation may be needed.

## Proposed Code Diffs (apply manually / create PR)
Note: These are conservative, minimal diffs. Adjust imports/line numbers as needed when applying.

1) SecurityConfig: remove the global permitAll and explicitly permit known public routes, ensuring tenant routes remain protected.

- File reference: [`lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/security/SecurityConfig.java`](lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/security/SecurityConfig.java:1)

Replacement guidance (example patch snippet):

```java
// java
// Replace the block that currently contains:
// .requestMatchers("/actuator/**").permitAll()
// .requestMatchers("/swagger-ui/**", "/v3/api-docs/**", "/swagger-ui.html").permitAll()
// .requestMatchers("/api/messages/**").permitAll()
// .requestMatchers("/api/**").permitAll()
// .requestMatchers("/ai/**").permitAll()
// .requestMatchers("/**").permitAll()
// .requestMatchers("/api/tenants/**").authenticated()
//
// With a tighter ordering such as:
.requestMatchers("/actuator/**").permitAll()
.requestMatchers("/swagger-ui/**", "/v3/api-docs/**", "/swagger-ui.html").permitAll()
// Message API endpoints are public
.requestMatchers("/api/messages/**").permitAll()
// Common public health/ai endpoints — only explicitly named
.requestMatchers("/ai/health/**").permitAll()
.requestMatchers("/ai/public/**").permitAll()
// Protect API by default: require authentication for /api/** except explicitly allowed above
.requestMatchers("/api/tenants/**").authenticated()
.requestMatchers("/api/**").authenticated()
// Fallback: deny by default (no .requestMatchers("/**").permitAll())
```

Rationale:
- The original `.requestMatchers("/**").permitAll()` overrides authentication and undermines the filter's purpose.
- Explicitly permit only known public endpoints; protect `/api/**` by default.

2) JwtAuthenticationFilter: add richer debug logging at filter entry, log presence (not value) of Authorization header, method, path, query string, correlation id (if present), and log exception stack traces when DEBUG enabled.

- File reference: [`lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/security/JwtAuthenticationFilter.java`](lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/security/JwtAuthenticationFilter.java:1)

Suggested insertions:
- At start of doFilterInternal (immediately after method entry / before public path skip), add:

```java
// java
logger.debug("JwtAuthenticationFilter entry: method={} path={} query={} remoteAddr={} hasAuthHeader={}",
    request.getMethod(),
    request.getRequestURI(),
    request.getQueryString(),
    request.getRemoteAddr(),
    request.getHeader("Authorization") != null);
```

- In each JWT exception catch (ExpiredJwtException, SignatureException, MalformedJwtException, generic Exception), keep current warn/error lines but also log full stacktrace at DEBUG to aid diagnosis:

```java
// java
catch (ExpiredJwtException e) {
    logger.warn("JWT token expired: {}", e.getMessage());
    logger.debug("ExpiredJwtException stacktrace:", e);
    handleUnauthorized(response, "Token expired");
    return;
}
```

- In the outer catch (filter-level), ensure exception is logged with stacktrace and include request metadata:

```java
// java
} catch (Exception e) {
    logger.error("Authentication filter error: {} method={} path={} ", e.getMessage(), request.getMethod(), request.getRequestURI(), e);
    handleUnauthorized(response, "Authentication failed");
}
```

- Optional: in handleUnauthorized add response.setCharacterEncoding("UTF-8") and flush writer:

```java
// java
response.setCharacterEncoding("UTF-8");
response.getWriter().write(...);
response.getWriter().flush();
```

Rationale:
- Current logs show only message and sometimes exception; adding debug-level stack traces and request metadata will make post-mortem easier without leaking token data.

## Decisions (MCP sequentialthinking)
- Plan: produce a non-invasive PR that tightens matchers and improves filter logs so errors are actionable. (sequentialthinking thought 1)
- Expand: propose specific matcher ordering and debug logging additions to JwtAuthenticationFilter to capture method/path and show stack traces under DEBUG.
- Verify: attempted to apply edits via `apply_diff`, but x51Cto is restricted to documentation edits only. The attempted apply failed with: Tool restriction prevents editing Java sources from this mode. Therefore edits must be applied by a developer with write access to source.
- Execute: since apply_diff could not be used here, this PRD documents exact patches to commit in a code PR. Marked as FALLBACK: no direct code execution by x51Cto.
- Reflect: this approach keeps changes minimal and reversible, and surfaces the probable root cause: global permitAll masking auth rules plus insufficient debug info in filter.

## Consistency & Cleanup
- Remove or avoid `.requestMatchers("/**").permitAll()` in `SecurityConfig` — it is misleading and inconsistent with later `.authenticated()` calls. (ref: [`SecurityConfig.java`](lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/security/SecurityConfig.java:31))
- Centralize public-path list: `JwtAuthenticationFilter` uses `isPublicPath` with startsWith checks (≈ lines 37–46). Consider deriving these allowed public prefixes from `SecurityConfig` to avoid duplication.
- Add unit/integration tests:
  - A test for expired token path returns 401 with expected JSON body.
  - A test for malformed signature returns 401.
  - A smoke test for `/api/tenants/**` to verify authentication enforcement after config fix.
- Add a repository-local `GlobalExceptionHandler` if missing; otherwise verify which artifact supplies it in production.

## Follow-ups
- Follow-up-001: Ops — Confirm deployed artifact/classpath matches workspace. Priority: high. Acceptance:
  - Deployed JAR/classpath inspected; location of `GlobalExceptionHandler` identified or provenance confirmed.
- Follow-up-002: Dev — Create a small PR applying the diffs above to:
  - Replace `.requestMatchers("/**").permitAll()` with explicit permits and defaults.
  - Add debug logging to `JwtAuthenticationFilter` and update `handleUnauthorized`.
  Priority: medium. Owner: backend dev.
- Follow-up-003: Dev/Test — Add unit/integration tests for JWT failure modes and endpoint authorization. Priority: medium.
- Follow-up-004: Dev — Consider moving public-path definitions into a shared config to avoid duplication (JWT filter vs SecurityConfig). Priority: low.

## Reviewer checklist
- [ ] Project context and diffs align with the source files: [`JwtAuthenticationFilter.java`](lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/security/JwtAuthenticationFilter.java:53), [`SecurityConfig.java`](lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/security/SecurityConfig.java:31)
- [ ] Proposed diffs are copy-paste ready and compile (owner to adjust imports if necessary).
- [ ] Follow-ups assigned and prioritized.
- [ ] Tests added or scheduled for follow-up.
