#!/usr/bin/env bash
set -euo pipefail

echo "[i] Starting Steampipe service..."
# 서비스 시작 + 로그 팔로우
steampipe service start >/tmp/steampipe.log 2>&1 &
# 실시간 로그도 같이 보기(백그라운드)
steampipe service logs --follow >/tmp/steampipe.follow.log 2>&1 &

# 준비 대기 (최대 120초로 여유)
RETRIES=120
SLEEP=1
until steampipe query "select now()" >/dev/null 2>&1; do
  if [ $RETRIES -le 0 ]; then
    echo "[x] Steampipe service failed to become ready. Logs:"
    echo "----- steampipe.log -----"; tail -n +1 /tmp/steampipe.log || true
    echo "----- steampipe.follow.log (last 200 lines) -----"; tail -n 200 /tmp/steampipe.follow.log || true
    exit 1
  fi
  echo "[i] Waiting for Steampipe to be ready... ($RETRIES)"
  sleep $SLEEP
  RETRIES=$((RETRIES-1))
done
echo "[i] Steampipe is ready."

echo "[i] Launching API server on 0.0.0.0:${PORT}"
exec python -m uvicorn main:app --host 0.0.0.0 --port "${PORT}"
