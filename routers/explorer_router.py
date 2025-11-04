from fastapi import APIRouter, Query, Request, Response
import asyncio
import apps.explorer as explorer
from utils.etag_utils import etag_response

router = APIRouter()

async def _run_with_etag(request: Request, response: Response, fn, *args):
    data = await asyncio.to_thread(fn, *args)
    return etag_response(request, response, data)

@router.get("/explorer/s3/{bucket_name}")
async def s3_all_objects(bucket_name: str, request: Request, response: Response, prefix: str = "", max_keys: int = Query(10, le=10000000)):
    return await _run_with_etag(request, response, explorer.get_s3_all_objects_content, bucket_name, prefix, max_keys)

@router.get("/explorer/dynamodb/{table_name}")
async def dynamodb_items(table_name: str, request: Request, response: Response, limit: int = Query(50, le=200)):
    return await _run_with_etag(request, response, explorer.get_dynamodb_items, table_name, limit)

@router.get("/explorer/glue/{database_name}")
async def glue_explorer(
    database_name: str,
    request: Request, response: Response,
    table_name: str = None,
    max_keys: int = Query(20, le=100)
):
    return await _run_with_etag(request, response, explorer.get_glue_data, database_name, table_name, max_keys)

@router.get("/explorer/redshift")
async def redshift_explorer(
    request: Request, response: Response,
    endpoint: str = Query(..., description="Redshift 엔드포인트 주소"),
    port: int = Query(5439, description="Redshift 포트 (기본 5439)"),
    db_name: str = Query(..., description="Redshift DB 이름"),
    user: str = Query(..., description="Redshift 사용자 이름"),
    password: str = Query(..., description="Redshift 사용자 비밀번호"),
    table_name: str = Query(None, description="특정 테이블 이름 (없으면 전체 테이블 목록 조회)"),
    limit: int = Query(50, le=200, description="조회할 행 개수 (기본 50)")
):
    return await _run_with_etag(request, response, explorer.get_redshift_data, endpoint, port, db_name, user, password, table_name, limit)

@router.get("/explorer/kinesis/{stream_name}")
async def kinesis_explorer(
    stream_name: str,
    request: Request, response: Response,
    shard_id: str = None,
    limit: int = Query(20, le=100)
):
    return await _run_with_etag(request, response, explorer.get_kinesis_records, stream_name, shard_id, limit)

@router.get("/explorer/feature-group/{feature_group_name}")
async def feature_group_data(
    feature_group_name: str,
    request: Request, response: Response,
    max_keys: int = Query(20, le=100)
):
    return await _run_with_etag(request, response, explorer.get_feature_group_data, feature_group_name, max_keys)

@router.get("/explorer/rds/{db_identifier}")
async def rds_explorer(
    db_identifier: str,
    request: Request, response: Response,
    endpoint: str,
    port: int = 5432,
    db_name: str = "postgres",
    user: str = "postgres",
    password: str = Query(..., description="Database password"),
    table_name: str = None,
    limit: int = Query(50, le=200)
):
    return await _run_with_etag(request, response, explorer.get_rds_data, endpoint, port, db_name, user, password, table_name, limit)

@router.get("/explorer/msk/{cluster_arn}")
async def msk_explorer(cluster_arn: str, request: Request, response: Response, topic: str = None, limit: int = Query(20, le=100)):
    return await _run_with_etag(request, response, explorer.get_msk_records, cluster_arn, topic, limit)

@router.get("/explorer/elasticache/redis")
async def elasticache_redis_explorer(
    request: Request, response: Response,
    host: str,
    port: int = Query(6379, description="Redis 포트"),
    password: str | None = Query(None, description="Redis AUTH 비밀번호 (없으면 None)"),
    db: int = Query(0, description="Redis DB index"),
    pattern: str = Query("*", description="SCAN 매칭 패턴"),
    limit: int = Query(50, le=500, description="최대 키 개수"),
    per_collection_limit: int = Query(50, le=500, description="LIST/SET/ZSET/HASH 등 컬렉션 당 샘플 개수"),
):
    return await _run_with_etag(request, response, explorer.get_redis_data, host, port, password, db, pattern, limit, per_collection_limit)