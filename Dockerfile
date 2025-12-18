# Dockerfile
FROM python:3.12-slim AS builder

ARG REPO_URL="https://github.com/BOB-DSPM/DSPM_Data-Collector.git"
ARG REPO_REF="main"
ENV VIRTUAL_ENV=/opt/venv

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    STEAMPIPE_UPDATE_CHECK=false \
    PORT=8103 \
    STEAMPIPE_DB_HOST=127.0.0.1 \
    STEAMPIPE_DB_PORT=9193 \
    STEAMPIPE_DB_USER=steampipe \
    STEAMPIPE_DB_NAME=steampipe \
    CORS_DEFAULT_ORIGINS="http://localhost:3000,http://127.0.0.1:3000,http://localhost:8103,http://127.0.0.1:8103" \
    CORS_ALLOW_ALL= \
    AWS_DEFAULT_REGION=ap-northeast-2

# 필수 패키지 + tini + unzip (AWS CLI 설치용)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates procps gnupg unzip jq tar gzip bash tini git build-essential \
 && rm -rf /var/lib/apt/lists/*

# 애플리케이션 소스 다운로드 및 종속성 설치
WORKDIR /src
RUN git clone --depth 1 --branch "${REPO_REF}" "${REPO_URL}" code
WORKDIR /src/code
RUN python -m venv "${VIRTUAL_ENV}" \
 && . "${VIRTUAL_ENV}/bin/activate" \
 && pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt


FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    STEAMPIPE_UPDATE_CHECK=false \
    PORT=8103 \
    STEAMPIPE_DB_HOST=127.0.0.1 \
    STEAMPIPE_DB_PORT=9193 \
    STEAMPIPE_DB_USER=steampipe \
    STEAMPIPE_DB_NAME=steampipe \
    CORS_DEFAULT_ORIGINS="http://localhost:3000,http://127.0.0.1:3000,http://localhost:8103,http://127.0.0.1:8103" \
    CORS_ALLOW_ALL= \
    AWS_DEFAULT_REGION=ap-northeast-2 \
    VIRTUAL_ENV=/opt/venv

ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

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

# 빌드 산출물 복사
COPY --from=builder /src/code /app
COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}

# 비루트 유저
RUN useradd -ms /bin/bash appuser \
 && chown -R appuser:appuser /app

# ===== Steampipe 설치(루트로 바이너리 설치) =====
RUN set -e; \
    /bin/sh -c "$(curl -fsSL https://steampipe.io/install/steampipe.sh)" \
    || /bin/sh -c "$(curl -fsSL https://raw.githubusercontent.com/turbot/steampipe/main/scripts/install.sh)"

# 플러그인 설치/버전 확인은 비루트로 실행
RUN su - appuser -c "steampipe --version" \
 && su - appuser -c "steampipe plugin install aws"

RUN su - appuser -c "steampipe service start"

# 실행 전 AWS/Steampipe 자동 설정 스크립트 정리
RUN chmod +x /app/docker/aws-init.sh \
 && sed -i 's/\r$//' /app/docker/aws-init.sh

# 엔트리포인트 스크립트 권한/개행 정리 (없어도 빌드 계속 가도록 방어)
RUN if [ -f /app/entrypoint.sh ]; then \
      chmod +x /app/entrypoint.sh && \
      sed -i 's/\r$//' /app/entrypoint.sh && \
      sed -i '1s/^\xEF\xBB\xBF//' /app/entrypoint.sh ; \
    else \
      echo '#!/usr/bin/env bash' > /app/entrypoint.sh && \
      echo 'UVICORN_BIN="${VIRTUAL_ENV:-/opt/venv}/bin/uvicorn"' >> /app/entrypoint.sh && \
      echo 'exec "${UVICORN_BIN}" main:app --host 0.0.0.0 --port ${PORT:-8103}' >> /app/entrypoint.sh && \
      chmod +x /app/entrypoint.sh ; \
    fi

# 런타임은 비루트
USER appuser

EXPOSE 8103
HEALTHCHECK --interval=30s --timeout=5s --start-period=25s --retries=5 \
  CMD curl -sf http://127.0.0.1:${PORT}/health || exit 1

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["bash", "-lc", "/app/docker/aws-init.sh && /app/entrypoint.sh"]
