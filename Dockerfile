# Dockerfile
FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    STEAMPIPE_UPDATE_CHECK=false \
    PORT=8103 \
    STEAMPIPE_DB_HOST=127.0.0.1 \
    STEAMPIPE_DB_PORT=9193

# 필수 패키지 + tini + unzip (AWS CLI 설치용)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates procps gnupg unzip jq tar gzip bash tini \
 && rm -rf /var/lib/apt/lists/*

# ===== AWS CLI v2 설치 (전역) =====
RUN set -eux; \
    curl -sSLo /tmp/awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip; \
    cd /tmp && unzip -q awscliv2.zip; \
    /tmp/aws/install; \
    aws --version; \
    rm -rf /tmp/aws /tmp/awscliv2.zip

WORKDIR /app

# Python 의존성 먼저 설치 (캐시 최적화)
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

# 소스 복사
COPY . /app

# 비루트 유저
RUN useradd -ms /bin/bash appuser

# ===== Steampipe 설치(루트로 바이너리 설치) =====
RUN set -e; \
    /bin/sh -c "$(curl -fsSL https://steampipe.io/install/steampipe.sh)" \
    || /bin/sh -c "$(curl -fsSL https://raw.githubusercontent.com/turbot/steampipe/main/scripts/install.sh)"

# 플러그인 설치/버전 확인은 비루트로 실행
RUN su - appuser -c "steampipe --version" \
 && su - appuser -c "steampipe plugin install aws"

# 실행 전 AWS/Steampipe 자동 설정 스크립트 추가
COPY docker/aws-init.sh /app/docker/aws-init.sh
RUN chmod +x /app/docker/aws-init.sh \
 && chown -R appuser:appuser /app \
 && sed -i 's/\r$//' /app/docker/aws-init.sh

# ✅ (누락되었던 부분) entrypoint.sh 복사
#   레포에 docker/entrypoint.sh 가 실제로 존재해야 합니다.
COPY docker/entrypoint.sh /app/entrypoint.sh

# 기존 엔트리포인트 스크립트 권한/개행 정리 (없어도 빌드 계속 가도록 방어)
RUN if [ -f /app/entrypoint.sh ]; then \
      chmod +x /app/entrypoint.sh && \
      sed -i 's/\r$//' /app/entrypoint.sh && \
      sed -i '1s/^\xEF\xBB\xBF//' /app/entrypoint.sh ; \
    else \
      echo '#!/usr/bin/env bash' > /app/entrypoint.sh && \
      echo 'exec python -m uvicorn apps.main:app --host 0.0.0.0 --port ${PORT:-8103}' >> /app/entrypoint.sh && \
      chmod +x /app/entrypoint.sh ; \
    fi

# 런타임은 비루트
USER appuser

EXPOSE 8103
HEALTHCHECK --interval=30s --timeout=5s --start-period=25s --retries=5 \
  CMD curl -sf http://127.0.0.1:${PORT}/health || exit 1

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["bash", "-lc", "/app/docker/aws-init.sh && /app/entrypoint.sh"]