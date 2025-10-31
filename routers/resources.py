from __future__ import annotations
from fastapi import APIRouter, Request, Response
import asyncio
import apps.collector as collector
from utils.etag_utils import etag_response

# ⬇ 세션 캐시 헬퍼 추가
from utils.caching import maybe_return_cached, store_response_to_cache

router = APIRouter()
DEFAULT_TTL = 600  # 초

async def _run_with_cache_and_etag(
    request: Request,
    response: Response,
    fn,
    *args,
    ttl_sec: int = DEFAULT_TTL,
):
    # 1) 캐시 조회
    cached = await maybe_return_cached(request, response, ttl=ttl_sec)
    if cached is not None:
        return etag_response(request, response, cached)

    # 2) 원래 계산
    data = await asyncio.to_thread(fn, *args)

    # 3) 캐시에 저장
    store_response_to_cache(request, data)
    response.headers["Cache-Control"] = f"public, max-age={ttl_sec}"

    # 4) ETag 응답
    return etag_response(request, response, data)

@router.get("/s3-buckets")
async def s3_buckets(request: Request, response: Response):
    return await _run_with_cache_and_etag(request, response, collector.get_s3_buckets)

@router.get("/ebs-volumes")
async def ebs_volumes(request: Request, response: Response):
    return await _run_with_cache_and_etag(request, response, collector.get_ebs_volumes)

@router.get("/efs-filesystems")
async def efs_filesystems(request: Request, response: Response):
    return await _run_with_cache_and_etag(request, response, collector.get_efs_filesystems)

@router.get("/fsx-filesystems")
async def fsx_filesystems(request: Request, response: Response):
    return await _run_with_cache_and_etag(request, response, collector.get_fsx_filesystems)

@router.get("/rds-instances")
async def rds_instances(request: Request, response: Response):
    return await _run_with_cache_and_etag(request, response, collector.get_rds_instances)

@router.get("/dynamodb-tables")
async def dynamodb_tables(request: Request, response: Response):
    return await _run_with_cache_and_etag(request, response, collector.get_dynamodb_tables)

@router.get("/redshift-clusters")
async def redshift_clusters(request: Request, response: Response):
    return await _run_with_cache_and_etag(request, response, collector.get_redshift_clusters)

@router.get("/rds-snapshots")
async def rds_snapshots(request: Request, response: Response):
    return await _run_with_cache_and_etag(request, response, collector.get_rds_snapshots)

@router.get("/elasticache-clusters")
async def elasticache_clusters(request: Request, response: Response):
    return await _run_with_cache_and_etag(request, response, collector.get_elasticache_clusters)

@router.get("/glacier-vaults")
async def glacier_vaults(request: Request, response: Response):
    return await _run_with_cache_and_etag(request, response, collector.get_glacier_vaults)

@router.get("/backup-plans")
async def backup_plans(request: Request, response: Response):
    return await _run_with_cache_and_etag(request, response, collector.get_backup_plans)

@router.get("/feature-groups")
async def sagemaker_feature_groups(request: Request, response: Response):
    return await _run_with_cache_and_etag(request, response, collector.get_sagemaker_feature_group)

@router.get("/glue-databases")
async def glue_databases(request: Request, response: Response):
    return await _run_with_cache_and_etag(request, response, collector.get_glue_catalog_database)

@router.get("/kinesis-streams")
async def kinesis_streams(request: Request, response: Response):
    return await _run_with_cache_and_etag(request, response, collector.get_kinesis_stream)

@router.get("/msk-clusters")
async def msk_clusters(request: Request, response: Response):
    return await _run_with_cache_and_etag(request, response, collector.get_msk_cluster)

@router.get("/all-resources")
async def all_resources(request: Request, response: Response):
    # 1) 캐시 조회 (결합 응답도 캐시)
    cached = await maybe_return_cached(request, response, ttl=DEFAULT_TTL)
    if cached is not None:
        return etag_response(request, response, cached)

    # 2) 원래 계산 (병렬 수집)
    (
        s3_buckets,
        ebs_volumes,
        efs_filesystems,
        fsx_filesystems,
        rds_instances,
        rds_snapshots,
        dynamodb_tables,
        redshift_clusters,
        elasticache_clusters,
        glacier_vaults,
        backup_plans,
        feature_groups,
        glue_databases,
        kinesis_streams,
        msk_clusters,
    ) = await asyncio.gather(
        asyncio.to_thread(collector.get_s3_buckets),
        asyncio.to_thread(collector.get_ebs_volumes),
        asyncio.to_thread(collector.get_efs_filesystems),
        asyncio.to_thread(collector.get_fsx_filesystems),
        asyncio.to_thread(collector.get_rds_instances),
        asyncio.to_thread(collector.get_rds_snapshots),
        asyncio.to_thread(collector.get_dynamodb_tables),
        asyncio.to_thread(collector.get_redshift_clusters),
        asyncio.to_thread(collector.get_elasticache_clusters),
        asyncio.to_thread(collector.get_glacier_vaults),
        asyncio.to_thread(collector.get_backup_plans),
        asyncio.to_thread(collector.get_sagemaker_feature_group),
        asyncio.to_thread(collector.get_glue_catalog_database),
        asyncio.to_thread(collector.get_kinesis_stream),
        asyncio.to_thread(collector.get_msk_cluster),
    )

    data = {
        "s3_buckets": s3_buckets,
        "ebs_volumes": ebs_volumes,
        "efs_filesystems": efs_filesystems,
        "fsx_filesystems": fsx_filesystems,
        "rds_instances": rds_instances,
        "rds_snapshots": rds_snapshots,
        "dynamodb_tables": dynamodb_tables,
        "redshift_clusters": redshift_clusters,
        "elasticache_clusters": elasticache_clusters,
        "glacier_vaults": glacier_vaults,
        "backup_plans": backup_plans,
        "feature_groups": feature_groups,
        "glue_databases": glue_databases,
        "kinesis_streams": kinesis_streams,
        "msk_clusters": msk_clusters,
    }

    # 3) 캐시에 저장 + 헤더
    store_response_to_cache(request, data)
    response.headers["Cache-Control"] = f"public, max-age={DEFAULT_TTL}"

    # 4) ETag 응답
    return etag_response(request, response, data)
