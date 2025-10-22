#!/usr/bin/env bash
set -euo pipefail

PORT="${PORT:-8103}"
STEAMPIPE_DB_HOST="${STEAMPIPE_DB_HOST:-127.0.0.1}"
STEAMPIPE_DB_PORT="${STEAMPIPE_DB_PORT:-9193}"

log() { printf '[%s] %s\n' "$(date +'%F %T')" "$*" ; }

# 1) Steampipe 서비스 데몬 기동 (백그라운드)
log "Starting Steampipe service on ${STEAMPIPE_DB_HOST}:${STEAMPIPE_DB_PORT} ..."
steampipe service start --database-listen=0.0.0.0 --database-port="${STEAMPIPE_DB_PORT}" >/dev/null

# 종료시 깔끔 종료
cleanup() {
  log "Stopping Steampipe service ..."
  steampipe service stop >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

# 2) Steampipe DB 대기 (/dev/tcp 사용)
log "Waiting for Steampipe DB to be ready ..."
RETRIES=50
SLEEP=0.3
for i in $(seq 1 $RETRIES); do
  if (echo >/dev/tcp/"${STEAMPIPE_DB_HOST}"/"${STEAMPIPE_DB_PORT}") >/dev/null 2>&1; then
    log "Steampipe DB is ready"
    break
  fi
  sleep "$SLEEP"
  if [[ "$i" -eq "$RETRIES" ]]; then
    log "ERROR: Steampipe DB not ready on ${STEAMPIPE_DB_HOST}:${STEAMPIPE_DB_PORT}"
    steampipe service status || true
    exit 1
  fi
done

# 3) Collector API 기동 (uvicorn)
#    ※ 모듈/경로는 실제 앱 엔트리포인트로 맞춰주세요.
log "Starting Collector API on 0.0.0.0:${PORT} ..."
exec uvicorn main:app --host 0.0.0.0 --port "${PORT}" --workers 2
