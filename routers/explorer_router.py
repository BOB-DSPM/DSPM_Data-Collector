# explorer_router.py
from fastapi import APIRouter, Query
import asyncio
import apps.explorer as explorer


router = APIRouter()

@router.get("/explorer/s3/{bucket_name}")
async def s3_objects(bucket_name: str, prefix: str = "", max_keys: int = Query(100, le=1000)):
    """
    선택한 S3 버킷 내부 객체(Object) 리스트 조회
    """
    return await asyncio.to_thread(explorer.get_s3_objects, bucket_name, prefix, max_keys)
