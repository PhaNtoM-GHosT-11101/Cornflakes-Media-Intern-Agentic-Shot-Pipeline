#!/usr/bin/env bash
# Build the image, smoke-test the published stack, then push to Docker Hub.
#
#   ./publish.sh
#
# Requires Docker access (your user in the `docker` group) and `docker login`.
set -euo pipefail
cd "$(dirname "$0")"

IMAGE="${IMAGE:-phantomghost11101/cornflakes-shot-pipeline:latest}"
APP="${IMAGE%%:*}"
COMPOSE="compose.published.yaml"

if curl -fsS -m 2 http://127.0.0.1:8000/healthz >/dev/null 2>&1; then
  echo "Something is already serving on :8000 — stop the local stack first (Ctrl+C)." >&2
  exit 1
fi

echo "==> building $IMAGE"
docker build -t "$IMAGE" .

echo "==> smoke-testing the published stack"
APP_IMAGE="$IMAGE" docker compose -f "$COMPOSE" up -d --wait
cleanup() { APP_IMAGE="$IMAGE" docker compose -f "$COMPOSE" down -v >/dev/null 2>&1 || true; }
trap cleanup EXIT

curl -fsS http://127.0.0.1:8000/healthz && echo
JOB=$(curl -fsS -X POST http://127.0.0.1:8000/v1/jobs \
  -H 'content-type: application/json' \
  -H "Idempotency-Key: publish-smoke-$$" \
  -d '{"story":"a docker smoke test on a paper boat","shots":2}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["job_id"])')
echo "    job: $JOB"

status=""
for _ in $(seq 1 60); do
  status=$(curl -fsS "http://127.0.0.1:8000/v1/jobs/$JOB" \
    | python3 -c 'import sys,json;print(json.load(sys.stdin)["job"]["status"])')
  [ "$status" = "done" ] && break
  sleep 1
done
if [ "$status" != "done" ]; then
  echo "smoke job did not finish (status=$status)" >&2
  exit 1
fi
echo "    smoke test OK"

echo "==> pushing $IMAGE"
docker push "$IMAGE"
echo "==> done: https://hub.docker.com/r/$APP"
