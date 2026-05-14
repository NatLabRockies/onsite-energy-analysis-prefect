#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
IMAGE_NAME=${IMAGE_NAME:-onsite-prefect-worker:latest}

DOCKER_BUILDKIT=1 docker build -f "$REPO_ROOT/docker/Dockerfile-minimal" -t "$IMAGE_NAME" "$REPO_ROOT"
