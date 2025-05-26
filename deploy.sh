#!/bin/bash
# filepath: /home/beou/IdeaProjects/lucy/lucid-backend2/lucid-ai-routing-service/deploy.sh

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
SERVICE_NAME="lucid-ai-routing-service"
IMAGE_NAME="lucid/${SERVICE_NAME}"
CONTAINER_NAME="${SERVICE_NAME}-container"
PORT=8083

echo -e "${GREEN}Starting deployment of ${SERVICE_NAME}...${NC}"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}Error: Docker is not running${NC}"
    exit 1
fi

# Build the application
echo -e "${YELLOW}Building application...${NC}"
./mvnw clean package -DskipTests

# Check if JAR file exists
JAR_FILE="target/${SERVICE_NAME}-*.jar"
if ! ls $JAR_FILE 1> /dev/null 2>&1; then
    echo -e "${RED}Error: JAR file not found in target directory${NC}"
    exit 1
fi

# Build Docker image
echo -e "${YELLOW}Building Docker image...${NC}"
docker build -t $IMAGE_NAME .

# Stop and remove existing container if it exists
if docker ps -a --format 'table {{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo -e "${YELLOW}Stopping existing container...${NC}"
    docker stop $CONTAINER_NAME
    docker rm $CONTAINER_NAME
fi

# Run the new container
echo -e "${YELLOW}Starting new container...${NC}"
docker run -d \
  --name $CONTAINER_NAME \
  --restart unless-stopped \
  -p $PORT:$PORT \
  -e SPRING_PROFILES_ACTIVE=docker \
  -e H2_PASSWORD=${H2_PASSWORD:-secure123} \
  -e GEMINI_API_KEY=${GEMINI_API_KEY:-} \
  -e OPENAI_API_KEY=${OPENAI_API_KEY:-} \
  -e LANGCHAIN_SERVICE_URL=${LANGCHAIN_SERVICE_URL:-http://localhost:8000} \
  -v ${PWD}/logs:/app/logs \
  $IMAGE_NAME

# Wait for the service to start
echo -e "${YELLOW}Waiting for service to start...${NC}"
sleep 10

# Health check
echo -e "${YELLOW}Performing health check...${NC}"
for i in {1..30}; do
    if curl -f http://localhost:$PORT/ai/health > /dev/null 2>&1; then
        echo -e "${GREEN}Service is healthy!${NC}"
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e "${RED}Health check failed after 30 attempts${NC}"
        echo -e "${YELLOW}Container logs:${NC}"
        docker logs $CONTAINER_NAME
        exit 1
    fi
    echo "Attempt $i/30 failed, retrying in 2 seconds..."
    sleep 2
done

echo -e "${GREEN}Deployment completed successfully!${NC}"
echo -e "${GREEN}Service is running at: http://localhost:$PORT${NC}"
echo -e "${GREEN}Health endpoint: http://localhost:$PORT/ai/health${NC}"
echo -e "${GREEN}API documentation: http://localhost:$PORT/swagger-ui.html${NC}"

# Show running container
echo -e "${YELLOW}Container status:${NC}"
docker ps --filter "name=${CONTAINER_NAME}" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
