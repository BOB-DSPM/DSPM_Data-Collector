#!/usr/bin/env bash
set -euo pipefail

STEAMPIPE_BIN="${STEAMPIPE_BIN:-steampipe}"
UVICORN_BIN="${VIRTUAL_ENV:-/opt/venv}/bin/uvicorn"
PORT="${PORT:-8103}"

# Start Steampipe service if not already running, then wait briefly for readiness.
if ! ${STEAMPIPE_BIN} service status >/dev/null 2>&1; then
  ${STEAMPIPE_BIN} service start >/tmp/steampipe-service.log 2>&1 &
fi

# Wait until the service reports healthy (best-effort, do not block forever)
for _ in $(seq 1 20); do
  if ${STEAMPIPE_BIN} service status >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done

exec "${UVICORN_BIN}" main:app --host 0.0.0.0 --port "${PORT}"
