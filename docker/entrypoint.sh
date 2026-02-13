#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="/opt/comfy_endpoints"
CONTRACT_PATH="${COMFY_ENDPOINTS_CONTRACT_PATH:-/opt/app/workflow.contract.json}"
WORKFLOW_PATH="${COMFY_ENDPOINTS_WORKFLOW_PATH:-/opt/app/workflow.json}"
CACHE_ROOT="${COMFY_ENDPOINTS_CACHE_ROOT:-/cache}"
WATCH_PATHS="${COMFY_ENDPOINTS_WATCH_PATHS:-/opt/comfy/models}"
MIN_FILE_SIZE_MB="${COMFY_ENDPOINTS_MIN_FILE_SIZE_MB:-100}"
GATEWAY_PORT="${COMFY_ENDPOINTS_GATEWAY_PORT:-3000}"
GATEWAY_KEY="${COMFY_ENDPOINTS_API_KEY:-change-me}"
APP_ID="${COMFY_ENDPOINTS_APP_ID:-}"

python -m comfy_endpoints.deploy.bootstrap \
  --cache-root "${CACHE_ROOT}" \
  --watch-paths "${WATCH_PATHS}" \
  --min-file-size-mb "${MIN_FILE_SIZE_MB}" \
  --contract-path "${CONTRACT_PATH}" \
  --workflow-path "${WORKFLOW_PATH}" \
  --api-key "${GATEWAY_KEY}" \
  --gateway-port "${GATEWAY_PORT}" \
  --app-id "${APP_ID}"
