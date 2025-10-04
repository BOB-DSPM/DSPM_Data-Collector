# repository.py
from fastapi import APIRouter, HTTPException
import asyncio
import apps.inspector as inspector

router = APIRouter()

@router.get("/repositories/s3/{bucket_name}")
async def s3_bucket_detail(bucket_name: str):
    return await asyncio.to_thread(inspector.get_s3_bucket_detail, bucket_name)

@router.get("/repositories/efs/{file_system_id}")
async def efs_detail(file_system_id: str):
    return await asyncio.to_thread(inspector.get_efs_filesystem_detail, file_system_id)

@router.get("/repositories/fsx/{file_system_id}")
async def fsx_detail(file_system_id: str):
    return await asyncio.to_thread(inspector.get_fsx_filesystem_detail, file_system_id)

@router.get("/repositories/rds/{db_identifier}")
async def rds_detail(db_identifier: str):
    return await asyncio.to_thread(inspector.get_rds_instance_detail, db_identifier)

@router.get("/repositories/dynamodb/{table_name}")
async def dynamodb_detail(table_name: str):
    return await asyncio.to_thread(inspector.get_dynamodb_table_detail, table_name)

@router.get("/repositories/redshift/{cluster_id}")
async def redshift_detail(cluster_id: str):
    return await asyncio.to_thread(inspector.get_redshift_cluster_detail, cluster_id)

@router.get("/repositories/rds-snapshot/{snapshot_id}")
async def rds_snapshot_detail(snapshot_id: str):
    return await asyncio.to_thread(inspector.get_rds_snapshot_detail, snapshot_id)

@router.get("/repositories/elasticache/{cluster_id}")
async def elasticache_detail(cluster_id: str):
    return await asyncio.to_thread(inspector.get_elasticache_cluster_detail, cluster_id)

@router.get("/repositories/glacier/{vault_name}")
async def glacier_detail(vault_name: str):
    return await asyncio.to_thread(inspector.get_glacier_vault_detail, vault_name)

@router.get("/repositories/backup/{plan_id}")
async def backup_detail(plan_id: str):
    return await asyncio.to_thread(inspector.get_backup_plan_detail, plan_id)

@router.get("/repositories/feature-group/{feature_group_name}")
async def feature_group_detail(feature_group_name: str):
    return await asyncio.to_thread(inspector.get_sagemaker_feature_group_detail, feature_group_name)

@router.get("/repositories/glue/{name}")
async def glue_database_detail(name: str):
    return await asyncio.to_thread(inspector.get_glue_database_detail, name)

@router.get("/repositories/kinesis/{stream_name}")
async def kinesis_detail(stream_name: str):
    return await asyncio.to_thread(inspector.get_kinesis_stream_detail, stream_name)

@router.get("/repositories/msk/{cluster_name}")
async def msk_detail(cluster_name: str):
    return await asyncio.to_thread(inspector.get_msk_cluster_detail, cluster_name)
