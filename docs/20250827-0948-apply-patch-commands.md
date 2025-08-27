# Apply patch and create branch (instructions)

Reference patch: [`lucid-ai-routing-service/docs/20250827-0948-security-jwt.patch`](lucid-ai-routing-service/docs/20250827-0948-security-jwt.patch:1)
Reference PRD: [`lucid-ai-routing-service/docs/20250827-0948-tighten-security-jwt-logging-PRD.md`](lucid-ai-routing-service/docs/20250827-0948-tighten-security-jwt-logging-PRD.md:1)

Steps — run these from the repository root:

```bash
git fetch origin
git checkout -b fix/security-tighten-jwt-20250827
git apply lucid-ai-routing-service/docs/20250827-0948-security-jwt.patch
git add -A
git commit -m "Tighten SecurityConfig and enhance JwtAuthenticationFilter logging (see docs/20250827-0948-tighten-security-jwt-logging-PRD.md)"
mvn -pl lucid-ai-routing-service -am test
git push origin fix/security-tighten-jwt-20250827
gh pr create --title "Tighten security and enhance JWT logging" --body-file=lucid-ai-routing-service/docs/20250827-0948-tighten-security-jwt-logging-PRD.md
```

Reproduction note — observed 401

The user reports a 401 when calling /api/auth/users/me. Example (token redacted):

```bash
curl --location 'https://dev.deemerge.ai/api/auth/users/me' \
--header 'authorization: Bearer <REDACTED_JWT_TOKEN>' \
--header 'accept: application/json, text/plain, */*'
```

Ask developer: reproduce and collect evidence

- Reproduce the request locally or against dev environment using the exact token (do not paste secrets in PR comments; provide sanitized logs).
- Enable DEBUG logging for the security package:
  - set logging.level.com.lucid.automation.airouting.security=DEBUG in lucid-ai-routing-service/src/main/resources/application-local.yml or
  - start app with JVM property: -Dlogging.level.com.lucid.automation.airouting.security=DEBUG
- Collect full application logs during the request (stdout and file) and attach to PR.
- Capture JwtAuthenticationFilter processing lines (see [`lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/security/JwtAuthenticationFilter.java`](lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/security/JwtAuthenticationFilter.java:1))
- Capture effective security matcher config (see [`lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/security/SecurityConfig.java`](lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/security/SecurityConfig.java:1))
- Inspect logs for exceptions: ExpiredJwtException, SignatureException, MalformedJwtException, or general errors. Provide stack traces.
- Verify token claims (exp, iss, aud) and time skew; confirm signature verification against JWKS.
- Confirm deployed artifact matches workspace:
  - run git rev-parse HEAD on the deployment build node or compare jar MANIFEST.MF
  - verify jar timestamp / git commit embedded in the artifact

What to include in PR comments (attach as files or paste sanitized snippets):

- Exact curl command used (with token redacted)
- Response status, response headers, and full response body
- Collected logs (DEBUG lines), with timestamps and file references
- Jwt token claims and verification results (sanitized)
- Hypothesis and recommended next steps

Short checklist for developer:

- [ ] Reproduce 401 locally with same token
- [ ] Collect JwtAuthenticationFilter debug logs (see file link)
- [ ] Confirm SecurityConfig effective matchers
- [ ] Confirm deployed artifact matches repo
- [ ] Post findings to PR body and link to PRD

Links:
- JwtAuthenticationFilter: [`lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/security/JwtAuthenticationFilter.java`](lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/security/JwtAuthenticationFilter.java:1)
- SecurityConfig: [`lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/security/SecurityConfig.java`](lucid-ai-routing-service/src/main/java/com/lucid/automation/airouting/security/SecurityConfig.java:1)