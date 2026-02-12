#!/usr/bin/env bash
set -euo pipefail

KEYCHAIN_SERVICE="${COMFY_ENDPOINTS_RUNPOD_KEYCHAIN_SERVICE:-COMFY_ENDPOINTS_RUNPOD_API_KEY}"
KEYCHAIN_ACCOUNT="${COMFY_ENDPOINTS_RUNPOD_KEYCHAIN_ACCOUNT:-${USER:-}}"

if [[ -z "${KEYCHAIN_ACCOUNT}" ]]; then
  echo "COMFY_ENDPOINTS_RUNPOD_KEYCHAIN_ACCOUNT or USER must be set" >&2
  exit 1
fi

export RUNPOD_API_KEY="$(security find-generic-password -a "${KEYCHAIN_ACCOUNT}" -s "${KEYCHAIN_SERVICE}" -w)"
echo "RUNPOD_API_KEY loaded from Keychain service '${KEYCHAIN_SERVICE}' for account '${KEYCHAIN_ACCOUNT}'."
