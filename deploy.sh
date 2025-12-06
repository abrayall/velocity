#!/bin/bash

# Velocity Server Deploy Script
# Creates GitHub release, builds and pushes Docker image to DigitalOcean registry

set -e

# Colors
GREEN='\033[38;2;39;201;63m'
YELLOW='\033[38;2;222;184;65m'
BLUE='\033[38;2;59;130;246m'
GRAY='\033[38;2;136;136;136m'
RED='\033[0;31m'
NC='\033[0m'

# Registry configuration
REGISTRY="registry.digitalocean.com"
REPO="abrayall"
IMAGE="velocity-server"
GITHUB_REPO="abrayall/velocity"

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Work directory
WORK_DIR="$SCRIPT_DIR/build/work"
mkdir -p "$WORK_DIR"

echo "=============================================="
echo -e "${YELLOW}Velocity Server Deploy${NC}"
echo "=============================================="
echo ""

# Get version using vermouth
VERSION=$(vermouth 2>/dev/null || curl -sfL https://raw.githubusercontent.com/abrayall/vermouth/refs/heads/main/vermouth.sh | sh -)

# Check if this is a release version (no hyphen means it's a tag like 0.1.0)
IS_RELEASE=false
if [[ ! "$VERSION" =~ "-" ]]; then
    IS_RELEASE=true
fi

# Full image names
VERSION_TAG="${REGISTRY}/${REPO}/${IMAGE}:${VERSION}"
LATEST_TAG="${REGISTRY}/${REPO}/${IMAGE}:latest"

echo -e "${BLUE}Version:${NC}  ${VERSION}"
echo -e "${BLUE}Release:${NC}  ${IS_RELEASE}"
echo -e "${BLUE}Registry:${NC} ${REGISTRY}/${REPO}"
echo -e "${BLUE}Image:${NC}    ${IMAGE}"
echo ""

# =============================================================================
# GitHub Release (only for release versions)
# =============================================================================

REGISTRY_TOKEN="${REGISTRY_TOKEN:-$GITHUB_TOKEN}"
if [ "$IS_RELEASE" = true ] && [ -n "$REGISTRY_TOKEN" ]; then
    echo -e "${YELLOW}Creating GitHub Release...${NC}"

    # Check if release already exists
    RELEASE_CHECK=$(curl -s -o /dev/null -w "%{http_code}" \
        -H "Authorization: token $REGISTRY_TOKEN" \
        -H "Accept: application/vnd.github.v3+json" \
        "https://api.github.com/repos/${GITHUB_REPO}/releases/tags/v${VERSION}")

    if [ "$RELEASE_CHECK" = "200" ]; then
        echo -e "${GRAY}  Release v${VERSION} already exists, skipping creation${NC}"
    else
        # Create release
        RELEASE_RESPONSE=$(curl -s -X POST \
            -H "Authorization: token $REGISTRY_TOKEN" \
            -H "Accept: application/vnd.github.v3+json" \
            "https://api.github.com/repos/${GITHUB_REPO}/releases" \
            -d "{\"tag_name\":\"v${VERSION}\",\"name\":\"v${VERSION}\",\"body\":\"Release v${VERSION}\",\"draft\":false,\"prerelease\":false}")

        RELEASE_ID=$(echo "$RELEASE_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null)

        if [ -n "$RELEASE_ID" ] && [ "$RELEASE_ID" != "None" ]; then
            echo -e "${GREEN}✓ Created release v${VERSION}${NC}"

            # Upload build artifacts if they exist
            if [ -d "$SCRIPT_DIR/build" ]; then
                echo -e "${BLUE}Uploading release assets...${NC}"

                for ASSET in "$SCRIPT_DIR/build"/velocity-*; do
                    if [ -f "$ASSET" ]; then
                        ASSET_NAME=$(basename "$ASSET")
                        echo -e "  Uploading ${GRAY}${ASSET_NAME}${NC}..."

                        curl -s -X POST \
                            -H "Authorization: token $REGISTRY_TOKEN" \
                            -H "Content-Type: application/octet-stream" \
                            --data-binary "@${ASSET}" \
                            "https://uploads.github.com/repos/${GITHUB_REPO}/releases/${RELEASE_ID}/assets?name=${ASSET_NAME}" > /dev/null
                    fi
                done

                echo -e "${GREEN}✓ Uploaded release assets${NC}"
            fi
        else
            echo -e "${RED}✗ Failed to create release${NC}"
            echo -e "${GRAY}  Response: $RELEASE_RESPONSE${NC}"
        fi
    fi
    echo ""
elif [ "$IS_RELEASE" = true ]; then
    echo -e "${GRAY}No REGISTRY_TOKEN set, skipping GitHub release${NC}"
    echo ""
fi

# =============================================================================
# Docker Build & Push
# =============================================================================

# Generate Dockerfile
echo -e "${BLUE}Generating Dockerfile...${NC}"
cat > "$WORK_DIR/Dockerfile" << 'DOCKERFILE'
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files first (cached layer)
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY server/ server/
COPY internal/ internal/

# Build server
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags "-X velocity/internal/version.Version=${VERSION}" \
    -o /velocity-server \
    ./server

# Runtime image
FROM alpine:3.19

RUN apk --no-cache add ca-certificates

COPY --from=builder /velocity-server /usr/local/bin/velocity-server

EXPOSE 8080

ENTRYPOINT ["velocity-server"]
DOCKERFILE

# Build the Docker image
echo -e "${YELLOW}Building Docker image...${NC}"
echo ""

docker build \
    --platform linux/amd64 \
    --build-arg VERSION="${VERSION}" \
    -t "${VERSION_TAG}" \
    -t "${LATEST_TAG}" \
    -f "$WORK_DIR/Dockerfile" \
    .

echo ""
echo -e "${GREEN}✓ Built: ${VERSION_TAG}${NC}"
echo ""

# Login to registry
TOKEN="${DIGITALOCEAN_TOKEN:-$TOKEN}"
if [ -n "$TOKEN" ]; then
    echo -e "${BLUE}Logging in to registry...${NC}"
    echo "$TOKEN" | docker login "$REGISTRY" --username "$TOKEN" --password-stdin
    echo ""
else
    echo -e "${GRAY}No DIGITALOCEAN_TOKEN env var set, assuming already logged in${NC}"
fi

# Push to registry
echo -e "${YELLOW}Pushing to registry...${NC}"
echo ""

docker push "${VERSION_TAG}"
docker push "${LATEST_TAG}"

echo ""
echo -e "${GREEN}✓ Pushed images${NC}"
echo ""

# Check/create DigitalOcean App
APP_NAME="velocity-server"

if [ -z "$TOKEN" ]; then
    echo -e "${GRAY}No DIGITALOCEAN_TOKEN set, skipping app deployment${NC}"
else
    echo -e "${YELLOW}Checking DigitalOcean App Platform...${NC}"

    # Get all apps and search for our app by name
    APPS_RESPONSE=$(curl -s -X GET \
        -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/json" \
        "https://api.digitalocean.com/v2/apps")

    # Check if our app exists
    if echo "$APPS_RESPONSE" | grep -q "\"name\":\"$APP_NAME\""; then
        echo -e "${GREEN}✓ App '$APP_NAME' already exists${NC}"

        # Get the app URL
        APP_URL=$(echo "$APPS_RESPONSE" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for app in data.get('apps', []):
    if app.get('spec', {}).get('name') == '$APP_NAME':
        print(app.get('live_url', ''))
        break
" 2>/dev/null)

        if [ -n "$APP_URL" ]; then
            echo -e "${BLUE}  URL:${NC} $APP_URL"
        fi
        echo -e "${GRAY}  Deployment will be triggered automatically by deploy_on_push${NC}"
    else
        echo -e "${BLUE}Creating app '$APP_NAME'...${NC}"

        # Check for S3 credentials
        if [ -z "$S3_ACCESS_KEY_ID" ] || [ -z "$S3_SECRET_ACCESS_KEY" ]; then
            echo -e "${RED}✗ S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY env vars required to create app${NC}"
            echo -e "${GRAY}  Set them and run again:${NC}"
            echo -e "${GRAY}  export S3_ACCESS_KEY_ID=your-key${NC}"
            echo -e "${GRAY}  export S3_SECRET_ACCESS_KEY=your-secret${NC}"
            exit 1
        fi

        # Find the velocity project ID
        echo -e "${GRAY}Looking up velocity project...${NC}"
        PROJECTS_RESPONSE=$(curl -s -X GET \
            -H "Authorization: Bearer $TOKEN" \
            -H "Content-Type: application/json" \
            "https://api.digitalocean.com/v2/projects")

        PROJECT_ID=$(echo "$PROJECTS_RESPONSE" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for p in data.get('projects', []):
    if p.get('name', '').lower() == 'velocity':
        print(p.get('id', ''))
        break
" 2>/dev/null)

        if [ -z "$PROJECT_ID" ]; then
            echo -e "${YELLOW}⚠ Velocity project not found, app will be created in default project${NC}"
            PROJECT_FIELD=""
        else
            echo -e "${GREEN}✓ Found velocity project: ${PROJECT_ID}${NC}"
            PROJECT_FIELD="\"project_id\":\"${PROJECT_ID}\","
        fi

        # Create app spec with credentials from environment
        APP_SPEC="{${PROJECT_FIELD}\"spec\":{\"name\":\"velocity-server\",\"region\":\"nyc\",\"features\":[\"buildpack-stack=ubuntu-22\",\"disable-edge-cache\"],\"alerts\":[{\"rule\":\"DEPLOYMENT_FAILED\"},{\"rule\":\"DOMAIN_FAILED\"}],\"domains\":[{\"domain\":\"velocity.ee\",\"type\":\"PRIMARY\"},{\"domain\":\"api.velocity.ee\",\"type\":\"ALIAS\"}],\"ingress\":{\"rules\":[{\"component\":{\"name\":\"velocity-server\"},\"match\":{\"path\":{\"prefix\":\"/\"}}}]},\"services\":[{\"name\":\"velocity-server\",\"http_port\":8080,\"image\":{\"registry_type\":\"DOCR\",\"registry\":\"abrayall\",\"repository\":\"velocity-server\",\"tag\":\"latest\",\"deploy_on_push\":{\"enabled\":true}},\"envs\":[{\"key\":\"S3_ENDPOINT\",\"scope\":\"RUN_AND_BUILD_TIME\",\"value\":\"s3.wasabisys.com\"},{\"key\":\"S3_REGION\",\"scope\":\"RUN_AND_BUILD_TIME\",\"value\":\"us-east-1\"},{\"key\":\"S3_BUCKET\",\"scope\":\"RUN_AND_BUILD_TIME\",\"value\":\"velocity\"},{\"key\":\"S3_ACCESS_KEY_ID\",\"scope\":\"RUN_AND_BUILD_TIME\",\"type\":\"SECRET\",\"value\":\"${S3_ACCESS_KEY_ID}\"},{\"key\":\"S3_SECRET_ACCESS_KEY\",\"scope\":\"RUN_AND_BUILD_TIME\",\"type\":\"SECRET\",\"value\":\"${S3_SECRET_ACCESS_KEY}\"},{\"key\":\"ENVIRONMENT\",\"scope\":\"RUN_AND_BUILD_TIME\",\"value\":\"production\"},{\"key\":\"LOG_LEVEL\",\"scope\":\"RUN_AND_BUILD_TIME\",\"value\":\"info\"}],\"health_check\":{\"http_path\":\"/api/health\",\"initial_delay_seconds\":5,\"period_seconds\":10,\"timeout_seconds\":3,\"success_threshold\":1,\"failure_threshold\":3},\"instance_count\":1,\"instance_size_slug\":\"apps-s-1vcpu-0.5gb\"}]}}"

        RESPONSE=$(curl -s -X POST \
            -H "Authorization: Bearer $TOKEN" \
            -H "Content-Type: application/json" \
            -d "$APP_SPEC" \
            "https://api.digitalocean.com/v2/apps")

        if echo "$RESPONSE" | grep -q '"app"'; then
            echo -e "${GREEN}✓ App '$APP_NAME' created${NC}"

            # Extract app ID from response
            APP_ID=$(echo "$RESPONSE" | python3 -c "
import sys, json
data = json.load(sys.stdin)
print(data.get('app', {}).get('id', ''))
" 2>/dev/null)

            echo -e "${GRAY}  Waiting for deployment...${NC}"

            # Poll for deployment status
            LAST_STATUS=""
            TIMEOUT=300
            ELAPSED=0

            while [ $ELAPSED -lt $TIMEOUT ]; do
                STATUS_RESPONSE=$(curl -s -X GET \
                    -H "Authorization: Bearer $TOKEN" \
                    -H "Content-Type: application/json" \
                    "https://api.digitalocean.com/v2/apps/$APP_ID")

                CURRENT_STATUS=$(echo "$STATUS_RESPONSE" | python3 -c "
import sys, json
data = json.load(sys.stdin)
app = data.get('app', {})
deployment = app.get('active_deployment') or app.get('pending_deployment') or {}
print(deployment.get('phase', 'UNKNOWN'))
" 2>/dev/null)

                # Show status changes
                if [ "$CURRENT_STATUS" != "$LAST_STATUS" ]; then
                    case "$CURRENT_STATUS" in
                        "PENDING_BUILD") echo -e "${GRAY}  Status: Pending build...${NC}" ;;
                        "BUILDING") echo -e "${YELLOW}  Status: Building...${NC}" ;;
                        "PENDING_DEPLOY") echo -e "${GRAY}  Status: Pending deploy...${NC}" ;;
                        "DEPLOYING") echo -e "${YELLOW}  Status: Deploying...${NC}" ;;
                        "ACTIVE") echo -e "${GREEN}  Status: Active${NC}" ;;
                        "ERROR"|"FAILED") echo -e "${RED}  Status: Failed${NC}" ;;
                        *) echo -e "${GRAY}  Status: $CURRENT_STATUS${NC}" ;;
                    esac
                    LAST_STATUS="$CURRENT_STATUS"
                fi

                # Check for terminal states
                if [ "$CURRENT_STATUS" = "ACTIVE" ]; then
                    APP_URL=$(echo "$STATUS_RESPONSE" | python3 -c "
import sys, json
data = json.load(sys.stdin)
print(data.get('app', {}).get('live_url', ''))
" 2>/dev/null)
                    echo ""
                    echo -e "${GREEN}✓ Deployment successful!${NC}"
                    if [ -n "$APP_URL" ]; then
                        echo -e "${BLUE}  URL:${NC} $APP_URL"
                    fi
                    break
                fi

                if [ "$CURRENT_STATUS" = "ERROR" ] || [ "$CURRENT_STATUS" = "FAILED" ]; then
                    echo ""
                    echo -e "${RED}✗ Deployment failed${NC}"
                    break
                fi

                sleep 5
                ELAPSED=$((ELAPSED + 5))
            done

            if [ $ELAPSED -ge $TIMEOUT ]; then
                echo -e "${YELLOW}⚠ Deployment still in progress (timed out waiting)${NC}"
            fi
        else
            echo -e "${RED}✗ Failed to create app${NC}"
            ERROR_MSG=$(echo "$RESPONSE" | python3 -c "
import sys, json
data = json.load(sys.stdin)
print(data.get('message', data.get('id', 'Unknown error')))
" 2>/dev/null || echo "$RESPONSE")
            echo -e "${RED}  Error: $ERROR_MSG${NC}"
        fi
    fi
fi

echo ""
echo "=============================================="
echo -e "${GREEN}Deploy Complete!${NC}"
echo "=============================================="
echo ""

echo "Pushed images:"
echo "  • ${VERSION_TAG}"
echo "  • ${LATEST_TAG}"
echo ""
