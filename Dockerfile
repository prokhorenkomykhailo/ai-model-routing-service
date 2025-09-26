# Stage 1: Build
FROM eclipse-temurin:17-jdk-alpine AS builder

LABEL maintainer="Lucid Team <support@lucid.com>"
LABEL description="Lucid AI Routing Service - Build Stage"

WORKDIR /app

# Copy Maven wrapper and configuration files for ai-routing-service
COPY lucid-ai-routing-service/mvnw .
COPY lucid-ai-routing-service/mvnw.cmd .
COPY lucid-ai-routing-service/.mvn .mvn
COPY lucid-ai-routing-service/pom.xml .

# Copy shared DTOs project
COPY lucid-common-dtos ../lucid-common-dtos

# Copy source code
COPY lucid-ai-routing-service/src ./src

# Make mvnw executable, build shared DTOs first, then build the application
RUN chmod +x ./mvnw && \
    cd ../lucid-common-dtos && \
    chmod +x ./mvnw && \
    ./mvnw clean install -DskipTests && \
    cd /app && \
    ./mvnw clean package -Pdocker -DskipTests

# Stage 2: Runtime
FROM eclipse-temurin:17-jre-alpine

LABEL maintainer="Lucid Team <support@lucid.com>"
LABEL description="Lucid AI Routing Service"

# Create a non-root user to run the application
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app

# Add curl for healthcheck
RUN apk --no-cache add curl

# Copy the JAR file from build stage
COPY --from=builder /app/target/lucid-ai-routing-service-*.jar app.jar

# Set proper ownership
RUN chown -R appuser:appgroup /app

# Environment variables
ENV JAVA_OPTS=""
ENV SERVER_PORT=8083

EXPOSE 8083

# Switch to non-root user
USER appuser

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]

HEALTHCHECK --interval=30s --timeout=30s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:8083/actuator/health || exit 1
