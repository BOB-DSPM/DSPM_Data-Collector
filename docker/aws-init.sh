#!/usr/bin/env bash
set -euo pipefail

# appuser 홈
HOME_DIR="${HOME:-/home/appuser}"
AWS_DIR="${HOME_DIR}/.aws"
STEAMPIPE_CFG_DIR="${HOME_DIR}/.steampipe/config"

mkdir -p "${AWS_DIR}" "${STEAMPIPE_CFG_DIR}"

# ===== 1) AWS CLI non-interactive configure =====
# IAM Role (IRSA/ECS) 또는 마운트된 ~/.aws 파일을 그대로 사용.
# 필요한 경우 사용자가 직접 ${HOME}/.aws 를 마운트하거나 사전에 구성해야 함.
chmod 600 "${AWS_DIR}/credentials" 2>/dev/null || true
chmod 600 "${AWS_DIR}/config" 2>/dev/null || true

# 간단한 호출자 확인(실패해도 계속)
aws --version >/dev/null 2>&1 || true
aws sts get-caller-identity >/dev/null 2>&1 || true

# ===== 2) Steampipe aws 플러그인 기본 설정(리전/오류무시) =====
AWS_SPC="${STEAMPIPE_CFG_DIR}/aws.spc"
if [[ ! -f "${AWS_SPC}" ]]; then
  REGION_LIST="${AWS_DEFAULT_REGION:-ap-northeast-2}"
  echo "[i] creating ${AWS_SPC} (regions=${REGION_LIST})"
  cat > "${AWS_SPC}" <<EOF
connection "aws" {
  plugin  = "aws"
  regions = ["${REGION_LIST}"]
  ignore_error_codes = [
    "SubscriptionRequiredException",
    "OptInRequired",
    "UnauthorizedOperation",
    "AccessDenied",
    "AccessDeniedException"
  ]
}
EOF
fi

# steampipe 플러그인 업데이트는 선택(지연 방지 위해 실패 무시)
steampipe plugin update aws >/dev/null 2>&1 || true

# 참고: steampipe service는 기존 /app/entrypoint.sh 내부에서 기동
echo "[ok] aws-init complete"
