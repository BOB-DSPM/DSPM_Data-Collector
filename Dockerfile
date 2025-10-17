# Dockerfile
FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    STEAMPIPE_UPDATE_CHECK=false \
    PORT=8103

# 필수 패키지 (steampipe 설치 스크립트에 tar/gzip 필요) + bash 추가
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates procps gnupg unzip jq tar gzip bash \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python 의존성 먼저 설치 (캐시 최적화)
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

# 소스 복사
COPY . /app

# 비루트 유저 생성
RUN useradd -ms /bin/bash appuser

# Steampipe 설치 (root로 설치만 수행)
RUN set -e; \
    /bin/sh -c "$(curl -fsSL https://steampipe.io/install/steampipe.sh)" \
    || /bin/sh -c "$(curl -fsSL https://raw.githubusercontent.com/turbot/steampipe/main/scripts/install.sh)"

# 플러그인 설치/버전 확인은 비루트로 실행
RUN su - appuser -c "steampipe --version" \
 && su - appuser -c "steampipe plugin install aws"

# 엔트리포인트 스크립트 복사 & 권한 및 개행 정리(CRLF/BOM 제거)
COPY docker/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh \
 && chown appuser:appuser /app/entrypoint.sh \
 && sed -i 's/\r$//' /app/entrypoint.sh \
 && sed -i '1s/^\xEF\xBB\xBF//' /app/entrypoint.sh

# 런타임은 비루트
USER appuser

EXPOSE 8103
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=5 \
  CMD curl -sf http://127.0.0.1:${PORT}/health || exit 1

# ENTRYPOINT를 bash로 명시(개행/셸 문제 우회)
ENTRYPOINT ["bash", "/app/entrypoint.sh"]
