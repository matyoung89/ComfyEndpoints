#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/docker/docker-compose.stack.yml"
ENV_FILE="${COMFY_ENDPOINTS_ENV_FILE:-${ROOT_DIR}/.env.local}"

if [[ -f "${ENV_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a
fi

API_KEY="${COMFY_ENDPOINTS_API_KEY:-dev-api-key}"
PUBLIC_PORT="${COMFY_ENDPOINTS_PUBLIC_PORT:-18080}"
BASE_URL="http://127.0.0.1:${PUBLIC_PORT}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for smoke test" >&2
  exit 1
fi

export COMFY_ENDPOINTS_API_KEY="${API_KEY}"
export COMFY_ENDPOINTS_PUBLIC_PORT="${PUBLIC_PORT}"

cleanup()
{
  docker compose -f "${COMPOSE_FILE}" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

cd "${ROOT_DIR}/docker"
docker compose -f "${COMPOSE_FILE}" up -d --build

for _ in $(seq 1 30)
do
  if curl -fsS "${BASE_URL}/healthz" >/dev/null 2>&1
  then
    break
  fi
  sleep 1
done

curl -fsS "${BASE_URL}/healthz" | grep -q '"status"'

unauth_code="$(curl -s -o /dev/null -w '%{http_code}' -X POST "${BASE_URL}/run" -H 'content-type: application/json' -d '{"prompt":"hello"}')"
if [[ "${unauth_code}" != "401" ]]; then
  echo "Expected unauthorized run to return 401, got ${unauth_code}" >&2
  exit 1
fi

run_response="$(curl -fsS -X POST "${BASE_URL}/run" \
  -H 'content-type: application/json' \
  -H "x-api-key: ${API_KEY}" \
  -d '{"prompt":"hello"}')"

job_id="$(echo "${run_response}" | python -c 'import json,sys;print(json.load(sys.stdin)["job_id"])')"

for _ in $(seq 1 20)
do
  job_response="$(curl -fsS "${BASE_URL}/jobs/${job_id}" -H "x-api-key: ${API_KEY}")"
  job_state="$(echo "${job_response}" | python -c 'import json,sys;print(json.load(sys.stdin)["state"])')"
  if [[ "${job_state}" == "completed" ]]; then
    echo "Smoke test passed. job_id=${job_id}"
    exit 0
  fi
  sleep 1
done

echo "Smoke test failed: job did not reach completed state" >&2
exit 1
