# ComfyEndpoints

ComfyEndpoints provides a CLI-first workflow to deploy ComfyUI pipelines as hardened API endpoints.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
comfy-endpoints init /Users/mat/Projects/ComfyEndpoints/apps/demo
comfy-endpoints validate /Users/mat/Projects/ComfyEndpoints/apps/demo/app.yaml
comfy-endpoints deploy /Users/mat/Projects/ComfyEndpoints/apps/demo/app.yaml
```

## Commands

- `comfy-endpoints init <app_dir>`
- `comfy-endpoints validate <app_spec>`
- `comfy-endpoints deploy <app_spec>`
- `comfy-endpoints status <app_id> [--state-dir DIR]`
- `comfy-endpoints logs <app_id> [--state-dir DIR]`
- `comfy-endpoints destroy <app_spec>`

## RunPod Deployment Details

RunPod provider now performs concrete deploy operations:
1. Creates a pod with baseline config and mounted cache path (`/cache`).
2. Ensures pod volume size (patches volume when below requested size).
3. Patches pod image/env/ports for the resolved golden image.
4. Resumes pod and polls desired status.
5. Resolves external endpoint from `podHostId` when available.

If your image is private, set these in app spec `build`:
- `image_ref`: full container image ref
- `container_registry_auth_id`: RunPod registry auth ID for private registry pulls

Golden image handling during `deploy`:
1. Compute deterministic image tag from Comfy version + plugin refs + Dockerfile hash.
2. Check registry (`docker manifest inspect`) for that tag.
3. If missing:
   - use local Docker Buildx when available, or
   - dispatch GitHub Actions workflow (`.github/workflows/build_golden_image.yml`) when Docker is unavailable or backend is set to `github_actions`.
4. Deploy using the resolved image ref.

Note: auto build/push requires local Docker with Buildx available.

## Environment variables

- `RUNPOD_API_KEY`: API key for RunPod GraphQL API (highest priority source).
- `COMFY_ENDPOINTS_RUNPOD_KEYCHAIN_ACCOUNT`: optional override for Keychain account lookup (defaults to `$USER`).
- `COMFY_ENDPOINTS_ENV_FILE`: optional path to env file (default auto-discovery of `.env.local` then `.env`).
- `GHCR_USERNAME`: optional username for automatic `docker login ghcr.io` before image push.
- `GHCR_TOKEN`: optional token for automatic `docker login ghcr.io` before image push.
- `COMFY_ENDPOINTS_IMAGE_BUILD_BACKEND`: `auto` (default), `local`, or `github_actions`.
- `GITHUB_TOKEN`: required for remote GitHub Actions build dispatch.
- `GITHUB_REPOSITORY`: `owner/repo` required for remote GitHub Actions build dispatch.
- `COMFY_ENDPOINTS_GHA_WORKFLOW`: workflow filename (default `build_golden_image.yml`).
- `COMFY_ENDPOINTS_GHA_REF`: git ref for workflow dispatch (default `main`).

## RunPod API Key Storage (Safe)

ComfyEndpoints supports macOS Keychain fallback automatically in scripts/provider calls.

- Keychain service name: `COMFY_ENDPOINTS_RUNPOD_API_KEY`
- Keychain account: your macOS user (or `COMFY_ENDPOINTS_RUNPOD_KEYCHAIN_ACCOUNT`)

Store once:

```bash
security add-generic-password -a "$USER" -s COMFY_ENDPOINTS_RUNPOD_API_KEY -w "rp_..."
```

Verify access:

```bash
security find-generic-password -a "$USER" -s COMFY_ENDPOINTS_RUNPOD_API_KEY -w
```

Behavior at runtime:
1. If `RUNPOD_API_KEY` is set, ComfyEndpoints uses it.
2. Otherwise, ComfyEndpoints attempts to load `.env.local`/`.env` from the current project.
3. Otherwise, ComfyEndpoints reads from Keychain service `COMFY_ENDPOINTS_RUNPOD_API_KEY`.

Optional shell helper:

```bash
source /Users/mat/Projects/ComfyEndpoints/scripts/load_runpod_api_key.sh
```

`.env.local` example:

```bash
RUNPOD_API_KEY=rp_...
COMFY_ENDPOINTS_API_KEY=dev-api-key
COMFY_ENDPOINTS_PUBLIC_PORT=18080
```

Both CLI and provider flows auto-load `.env.local`, and `/Users/mat/Projects/ComfyEndpoints/scripts/smoke_test_local_stack.sh` also sources it automatically.

## Local Gateway + Proxy Smoke Test

Run a local sidecar-style stack (`gateway + nginx + comfy mock`) with:

```bash
/Users/mat/Projects/ComfyEndpoints/scripts/smoke_test_local_stack.sh
```

Compose stack files:
- `/Users/mat/Projects/ComfyEndpoints/docker/docker-compose.stack.yml`
- `/Users/mat/Projects/ComfyEndpoints/docker/nginx.compose.conf`
- `/Users/mat/Projects/ComfyEndpoints/docker/comfy_mock_server.py`
