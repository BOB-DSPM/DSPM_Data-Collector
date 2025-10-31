from __future__ import annotations
from typing import Any, Optional
from fastapi import Request, Response
from .session_cache import make_cache_key, cache_get, cache_set, DEFAULT_TTL_SEC

def _session_id_from(request: Request) -> Optional[str]:
    # 우선순위: X-Session-Id 헤더 > sid 쿠키
    return request.headers.get("X-Session-Id") or request.cookies.get("sid")

def compute_request_cache_key(request: Request, *, session_id: Optional[str]) -> str:
    q = dict(request.query_params)
    # 강제 새로고침 파라미터 제외
    q.pop("refresh", None)
    return make_cache_key(
        path=str(request.url.path),
        method=request.method,
        query=q,
        body=None,
        session_id=session_id,
    )

async def maybe_return_cached(request: Request, response: Response, *, ttl: int = DEFAULT_TTL_SEC) -> Any | None:
    # ?refresh=1 이면 캐시 무시
    if request.query_params.get("refresh") in ("1", "true", "True"):
        response.headers["X-Cache"] = "BYPASS"
        return None

    sid = _session_id_from(request)
    key = compute_request_cache_key(request, session_id=sid)
    cached = cache_get(key)
    if cached is not None:
        response.headers["X-Cache"] = "HIT"
        return cached

    # 키/TTL 저장 (핸들러가 계산 후 저장할 수 있게 state에 보관)
    request.state._cache_key = key
    request.state._cache_ttl = ttl
    response.headers["X-Cache"] = "MISS"
    return None

def store_response_to_cache(request: Request, payload: Any):
    key = getattr(request.state, "_cache_key", None)
    ttl = getattr(request.state, "_cache_ttl", DEFAULT_TTL_SEC)
    if key:
        cache_set(key, payload, ttl=ttl)
