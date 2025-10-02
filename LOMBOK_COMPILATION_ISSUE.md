# AI Routing Service - Lombok Compilation Issue

**Date**: October 2, 2025  
**Version**: 1.1.1 (attempted)  
**Status**: ⚠️ BLOCKED - Build Failure

## Problem Summary

After upgrading `lucid-common-dtos` from **v1.2.0** to **v1.3.0**, the AI routing service cannot compile due to widespread Lombok getter access failures.

## Error Details

**Compilation Errors**: 60+ errors across multiple files  
**Error Pattern**:
```
cannot find symbol
  symbol:   method getTs()
  location: class java.lang.Object
```

### Affected Files
- `IngestionConsumer.java` (12+ errors)
- `MessageService.java` (30+ errors)
- `ChannelService.java` (10+ errors)
- `UserService.java` (5+ errors)
- Pipeline processors:
  - `ValidationProcessor.java`
  - `UserProcessingProcessor.java`
  - `WorkspaceProcessingProcessor.java`
  - `RelatedUsersEnrichmentProcessor.java`
  - `MessageProcessingContext.java`
  - `IngestionProcessingContext.java`

### Missing Methods
All Lombok-generated getters from `MessageDTO` and related DTOs:
- `getTs()`
- `getChannelId()`
- `getChannelName()`
- `getTeamId()`
- `getTeamName()`
- `getText()`
- `getUser()`
- `getThreadTs()`
- `getType()`
- `getSubtype()`
- ... and 20+ more

## What Changed

### lucid-common-dtos v1.3.0 Changes
1. ✅ Added `sourceType` field to `IngestionEventDTO`
2. ✅ Added helper methods: `isSlackMessage()`, `isGmailMessage()`
3. ✅ Created `IngestionUserDTO` replacing `SlackUserDTO`
4. ✅ Lombok `@Data` annotation present on all DTOs

### AI Routing Service Changes (Attempted)
1. ✅ Updated pom.xml: `lucid-common-dtos` 1.2.0 → 1.3.0
2. ✅ Updated version: 1.1.0 → 1.1.1
3. ✅ Added reflection-based `sourceType` validation in `IngestionConsumer`
4. ❌ Build fails before JAR creation

## Root Cause Analysis

The error message "location: class java.lang.Object" indicates the compiler is treating DTO instances as `Object` type rather than their actual types. This suggests:

1. **Maven Dependency Cache Issue**: Local Maven repo may have corrupted metadata
2. **Lombok Annotation Processing**: Not running correctly for transitive dependencies
3. **Classpath Problem**: Updated DTO classes not being loaded properly
4. **Spring Boot Version Conflict**: Spring Boot 3.4.5 may have Lombok compatibility issues

## Attempted Solutions

### ✅ Tried
1. Rebuilt `lucid-common-dtos` with `mvn clean install` → DTO v1.3.0 installed to local repo
2. Updated AI routing pom.xml to reference v1.3.0
3. Ran `mvn clean compile` → Failed with Lombok errors
4. Purged and re-resolved dependencies → Same errors
5. Implemented reflection-based workaround for `sourceType` → Only addresses one field

### ❌ Not Attempted Yet
1. Complete Maven local repository purge: `rm -rf ~/.m2/repository/com/lucid/automation/`
2. Force reimport in IDE
3. Check Lombok version compatibility matrix with Spring Boot 3.4.5
4. Rebuild with explicit Lombok configuration in pom.xml
5. Try deploying lucid-common-dtos to remote Maven repository
6. Investigate if annotation processors are being discovered

## Workaround Status

### IngestionConsumer.java - Partial Fix Applied
```java
// Reflection-based sourceType access (works)
String sourceType = null;
try {
    java.lang.reflect.Method getSourceTypeMethod = 
        ingestionEventDto.getClass().getMethod("getSourceType");
    sourceType = (String) getSourceTypeMethod.invoke(ingestionEventDto);
} catch (Exception e) {
    sourceType = "SLACK"; // Backward compatibility
}
```

**Issue**: This approach only solves `sourceType` access. Applying reflection to all 60+ failing getter calls would:
- Massively degrade performance
- Make code unmaintainable
- Still indicate underlying Lombok configuration problem

## Impact Assessment

### ✅ Successfully Updated Services
- `lucid-slack-ingestion-service` v1.1.9 - **DEPLOYED**
- `lucid-gmail-ingestion-service` v1.2.2 - **DEPLOYED**
- Both services use defensive `sourceType` validation and are production-ready

### ❌ Blocked Service
- `lucid-ai-routing-service` v1.1.1 - **CANNOT BUILD**
- Currently runs v1.1.0 which expects old DTO structure
- Can still process messages but lacks `sourceType` validation

### Risk Analysis
**Immediate Risk**: LOW
- Producer services (Slack/Gmail) are sending messages with `sourceType` field
- AI routing v1.1.0 will receive messages with extra field but should ignore it (JSON deserialization tolerates extra fields)
- Core functionality continues to work

**Long-term Risk**: MEDIUM
- Cannot add new `sourceType`-aware features to AI routing
- Gmail message processing may not work correctly without validation
- Technical debt accumulates

## Recommended Next Steps

### Option 1: Deep Investigation (2-4 hours)
1. Completely purge local Maven cache for Lucid artifacts
2. Check Lombok annotation processor configuration
3. Verify Lombok version compatibility
4. Test with explicit annotation processor paths in pom.xml
5. Debug Maven compilation with `-X` flag

### Option 2: Isolate the Problem (1 hour)
1. Create minimal test project with same Spring Boot + Lombok versions
2. Import lucid-common-dtos v1.3.0
3. Test if getters are accessible
4. Identify exact configuration difference

### Option 3: Deferred Update (Immediate)
1. Revert AI routing service to use lucid-common-dtos v1.2.0
2. Keep version at 1.1.0
3. Deploy only Slack/Gmail updates
4. Schedule AI routing update as separate task
5. Document compatibility requirement

### Option 4: Manual Getter Implementation (4-6 hours, not recommended)
1. Remove Lombok `@Data` from DTOs
2. Manually write all getters/setters
3. Rebuild and test
4. High maintenance burden

## Current Recommendation

**Choose Option 3: Deferred Update**

**Rationale**:
- Slack and Gmail services successfully deployed with defensive validation ✅
- AI routing can continue running v1.1.0 without immediate issues
- Allows time for proper root cause investigation
- Avoids rushed fixes that may introduce bugs

**Action Items**:
1. Revert AI routing pom.xml changes
2. Keep IngestionConsumer reflection code (harmless if sourceType not used)
3. Document this issue in sprint retrospective
4. Schedule dedicated investigation session
5. Consider creating a reproducible test case for Lombok team if needed

## References

- Lombok compatibility: https://projectlombok.org/setup/maven
- Spring Boot 3.x Lombok issues: https://github.com/projectlombok/lombok/issues
- Maven annotation processors: https://maven.apache.org/plugins/maven-compiler-plugin/compile-mojo.html

---

**Status**: Work in Progress  
**Owner**: AI Routing Service Team  
**Priority**: Medium (not blocking production)
