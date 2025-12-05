#!/bin/bash

# Velocity CMS Build Script
# Builds the server binary for multiple platforms with version from git tags

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "=============================================="
echo -e "${YELLOW}Velocity Build${NC}"
echo "=============================================="
echo ""

# Create build directory
BUILD_DIR="build"
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

# Get version using vermouth
VERSION=$(vermouth 2>/dev/null || curl -sfL https://raw.githubusercontent.com/abrayall/vermouth/refs/heads/main/vermouth.sh | sh -)

echo -e "${GREEN}Building version: ${VERSION}${NC}"
echo ""

# Platforms to build for
PLATFORMS=("darwin/amd64" "darwin/arm64" "linux/amd64" "linux/arm64" "windows/amd64")

# Build binaries for each platform
echo -e "${BLUE}Building binaries...${NC}"

for PLATFORM in "${PLATFORMS[@]}"; do
    OS="${PLATFORM%/*}"
    ARCH="${PLATFORM#*/}"

    EXT=""
    if [ "$OS" == "windows" ]; then
        EXT=".exe"
    fi

    echo -e "  Building ${YELLOW}${OS}/${ARCH}${NC}..."

    # Build server
    GOOS=$OS GOARCH=$ARCH go build \
        -ldflags "-X velocity/internal/version.Version=${VERSION}" \
        -o "${BUILD_DIR}/velocity-server-${VERSION}-${OS}-${ARCH}${EXT}" \
        ./server

    # Build CLI
    GOOS=$OS GOARCH=$ARCH go build \
        -ldflags "-X velocity/internal/version.Version=${VERSION}" \
        -o "${BUILD_DIR}/velocity-cli-${VERSION}-${OS}-${ARCH}${EXT}" \
        ./cli
done

echo ""
echo -e "${GREEN}Build complete!${NC}"
echo ""
echo "Artifacts created in ${BUILD_DIR}/:"
for PLATFORM in "${PLATFORMS[@]}"; do
    OS="${PLATFORM%/*}"
    ARCH="${PLATFORM#*/}"
    EXT=""
    if [ "$OS" == "windows" ]; then
        EXT=".exe"
    fi
    echo "  - velocity-server-${VERSION}-${OS}-${ARCH}${EXT}"
    echo "  - velocity-cli-${VERSION}-${OS}-${ARCH}${EXT}"
done

# Create version file
echo "version=${VERSION}" > "${BUILD_DIR}/version.properties"
echo ""
echo -e "${BLUE}Version: ${VERSION}${NC}"
