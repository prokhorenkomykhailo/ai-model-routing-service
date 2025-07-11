# Use Eclipse Temurin JRE for smaller image size
FROM eclipse-temurin:17-jre-alpine

# Create a non-root user to run the application
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app

# Copy the JAR file
COPY target/lucid-ai-routing-service-*.jar app.jar

# Set proper ownership
RUN chown -R appuser:appgroup /app

EXPOSE 8083

# Switch to non-root user
USER appuser

ENTRYPOINT ["java", "-jar", "app.jar"]
