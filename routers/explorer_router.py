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

@router.get("/explorer/glue/{database_name}")
async def glue_explorer(
    database_name: str,
    table_name: str = None,
    max_keys: int = Query(20, le=100)
):
    return await asyncio.to_thread(explorer.get_glue_data, database_name, table_name, max_keys)

@router.get("/explorer/redshift")
async def redshift_explorer(
    endpoint: str = Query(..., description="Redshift 엔드포인트 주소"),
    port: int = Query(5439, description="Redshift 포트 (기본 5439)"),
    db_name: str = Query(..., description="Redshift DB 이름"),
    user: str = Query(..., description="Redshift 사용자 이름"),
    password: str = Query(..., description="Redshift 사용자 비밀번호"),
    table_name: str = Query(None, description="특정 테이블 이름 (없으면 전체 테이블 목록 조회)"),
    limit: int = Query(50, le=200, description="조회할 행 개수 (기본 50)")
):
    return await asyncio.to_thread(
        explorer.get_redshift_data,
        endpoint, port, db_name, user, password, table_name, limit
    )

@router.get("/explorer/kinesis/{stream_name}")
async def kinesis_explorer(
    stream_name: str,
    shard_id: str = None,
    limit: int = Query(20, le=100)
):
    return await asyncio.to_thread(
        explorer.get_kinesis_records, stream_name, shard_id, limit
    )