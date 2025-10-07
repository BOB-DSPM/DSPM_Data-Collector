# routers/explorer_router.py
from fastapi import APIRouter, Query
import asyncio
import apps.explorer as explorer

router = APIRouter()

# 버킷 안의 모든 객체 + 내용 조회
@router.get("/explorer/s3/{bucket_name}")
async def s3_all_objects(bucket_name: str, prefix: str = "", max_keys: int = Query(10, le=100)):
    return await asyncio.to_thread(explorer.get_s3_all_objects_content, bucket_name, prefix, max_keys)

@router.get("/explorer/dynamodb/{table_name}")
async def dynamodb_items(table_name: str, limit: int = Query(50, le=200)):
    return await asyncio.to_thread(explorer.get_dynamodb_items, table_name, limit)
