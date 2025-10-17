# etag_utils.py
import hashlib, json
from fastapi import Response, Request

def compute_obj_etag(obj) -> str:
    """
    응답 객체 전체를 안정적으로 직렬화해서 약한 ETag를 만든다.
    - 정렬된 JSON + compact separators
    - 직렬화 안 되는 타입은 default=str 로 처리
    """
    try:
        payload = json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    except TypeError:
        payload = json.dumps(obj, sort_keys=True, ensure_ascii=False, default=str, separators=(",", ":")).encode("utf-8")
    h = hashlib.sha1(payload).hexdigest()
    return f'W/"{h}:{len(payload)}"'

def etag_response(request: Request, response: Response, data):
    """
    If-None-Match 검사 → 동일하면 304, 아니면 ETag 부여 후 데이터 반환
    """
    etag = compute_obj_etag(data)
    inm = (request.headers.get("If-None-Match") or "").strip()
    if etag and inm == etag:
        response.headers["ETag"] = etag
        return Response(status_code=304)
    response.headers["ETag"] = etag
    response.headers["Cache-Control"] = "private, must-revalidate"
    return data
