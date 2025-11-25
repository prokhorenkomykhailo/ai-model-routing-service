#!/bin/bash

# Build and push Docker image for AI Routing Service
# Usage: ./build-docker.sh [tag]
# Example: ./build-docker.sh 1.1.0

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

SERVICE_NAME="lucid-ai-routing-service"
REGISTRY="docker.x51.vn/lucy"
VERSION="${1:-1.1.2}"
IMAGE_TAG="${REGISTRY}/${SERVICE_NAME}:${VERSION}"

echo -e "${GREEN}🏗️  Building AI Routing Service Docker Image${NC}"
echo -e "${YELLOW}📦 Service: ${SERVICE_NAME}${NC}"
echo -e "${YELLOW}🏷️  Tag: ${IMAGE_TAG}${NC}"
echo ""

# Step 1: Build the application with Maven
echo -e "${GREEN}Step 1/4: Building application with Maven...${NC}"
./mvnw clean compile package -Pdocker -DskipTests
if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Maven build failed${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Maven build successful${NC}"
echo ""

# Step 2: Build Docker image
echo -e "${GREEN}Step 2/4: Building Docker image...${NC}"
# Use Dockerfile (from parent context) for full builds
cd ..
docker build -f lucid-ai-routing-service/Dockerfile \
    -t ${IMAGE_TAG} \
    -t ${REGISTRY}/${SERVICE_NAME}:latest \
    .
if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Docker build failed${NC}"
    echo -e "${YELLOW}💡 Tip: For local builds with docker-compose, use Dockerfile.prebuilt${NC}"
    exit 1
fi
cd lucid-ai-routing-service
echo -e "${GREEN}✅ Docker image built successfully${NC}"
echo ""

# Step 3: Push to registry
echo -e "${GREEN}Step 3/4: Pushing to registry...${NC}"
docker push ${IMAGE_TAG}
if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Docker push failed${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Image pushed: ${IMAGE_TAG}${NC}"
echo ""

# Step 4: Push latest tag
echo -e "${GREEN}Step 4/4: Pushing latest tag...${NC}"
docker push ${REGISTRY}/${SERVICE_NAME}:latest
if [ $? -ne 0 ]; then
    echo -e "${YELLOW}⚠️  Warning: Failed to push latest tag (non-critical)${NC}"
fi
echo -e "${GREEN}✅ Latest tag pushed${NC}"
echo ""

echo -e "${GREEN}🎉 Build and push completed successfully!${NC}"
echo -e "${YELLOW}📋 Summary:${NC}"
echo -e "   Service: ${SERVICE_NAME}"
echo -e "   Version: ${VERSION}"
echo -e "   Image: ${IMAGE_TAG}"
echo -e ""
echo -e "${YELLOW}🚀 Next steps:${NC}"
echo -e "   1. Update docker-compose.yml to use version ${VERSION}"
echo -e "   2. Deploy: docker compose up -d ai-routing-service"
echo -e "   3. Check logs: docker compose logs -f ai-routing-service"
echo -e "   4. Verify health: curl http://localhost:8083/actuator/health"
