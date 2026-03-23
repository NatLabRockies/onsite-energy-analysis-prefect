#!/usr/bin/env bash
set -euo pipefail

PREFECT_WORK_POOL="${PREFECT_WORK_POOL:-julia-pool}"

uv run prefect work-pool create "$PREFECT_WORK_POOL" --type process --overwrite
uv run python -m onsite_prefect.deploy
