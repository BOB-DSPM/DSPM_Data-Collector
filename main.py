# main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import resources, repository, explorer_router
import os
from typing import List

app = FastAPI(title="AWS Resource Collector API")

# ── CORS 설정 ────────────────────────────────────────────────────────────────
def _parse_origins(raw: str) -> List[str]:
    return [o.strip() for o in raw.split(",") if o.strip()]

def _dedup_origins(origins: List[str]) -> List[str]:
    seen = set()
    result: List[str] = []
    for origin in origins:
        if origin not in seen:
            seen.add(origin)
            result.append(origin)
    return result

default_origin_env = os.getenv(
    "CORS_DEFAULT_ORIGINS",
    "http://localhost:3000,http://127.0.0.1:3000,http://localhost:8103,http://127.0.0.1:8103",
)
DEFAULT_ORIGINS = _dedup_origins(_parse_origins(default_origin_env))

# 배포/개발 환경에서 추가로 허용할 오리진을 쉼표로 주입 가능
extra = os.getenv("CORS_ALLOW_ORIGINS", "")
if extra.strip():
    DEFAULT_ORIGINS = _dedup_origins(DEFAULT_ORIGINS + _parse_origins(extra))

allow_all = os.getenv("CORS_ALLOW_ALL", "").lower() in ("1", "true", "yes")
allow_credentials = os.getenv("CORS_ALLOW_CREDENTIALS", "").lower() in ("1", "true", "yes")
allow_origins = ["*"] if allow_all and not allow_credentials else DEFAULT_ORIGINS

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=allow_credentials,
    allow_methods=["*"],             # 필요시 ["GET","POST","PUT","DELETE","OPTIONS"]
    allow_headers=["*"],             # Authorization, Content-Type 등
    # expose_headers=["ETag"],         # 프론트에서 읽어야 하는 응답 헤더 있으면 추가
    # max_age=86400,                   # 프리플라이트 캐시(초)
)
# ────────────────────────────────────────────────────────────────────────────

app.include_router(resources.router,  prefix="/api", tags=["AWS Resources"])
app.include_router(repository.router, prefix="/api", tags=["Repository Detail"])
app.include_router(explorer_router.router, prefix="/api", tags=["Repository Explorer"])

@app.get("/health", tags=["Health"])
async def health():
    return {"status": "ok", "message": "Service is healthy"}
