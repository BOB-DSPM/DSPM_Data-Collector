# resources.py
from fastapi import APIRouter
import asyncio
import apps.collector as collector

router = APIRouter()

@router.get("/s3-buckets")
async def s3_buckets():
    return await asyncio.to_thread(collector.get_s3_buckets)

@router.get("/ebs-volumes")
async def ebs_volumes():
    return await asyncio.to_thread(collector.get_ebs_volumes)

@router.get("/efs-filesystems")
async def efs_filesystems():
    return await asyncio.to_thread(collector.get_efs_filesystems)

@router.get("/fsx-filesystems")
async def fsx_filesystems():
    return await asyncio.to_thread(collector.get_fsx_filesystems)

@router.get("/rds-instances")
async def rds_instances():
    return await asyncio.to_thread(collector.get_rds_instances)

@router.get("/dynamodb-tables")
async def dynamodb_tables():
    return await asyncio.to_thread(collector.get_dynamodb_tables)

@router.get("/redshift-clusters")
async def redshift_clusters():
    return await asyncio.to_thread(collector.get_redshift_clusters)

@router.get("/rds-snapshots")
async def rds_snapshots():
    return await asyncio.to_thread(collector.get_rds_snapshots)

@router.get("/elasticache-clusters")
async def elasticache_clusters():
    return await asyncio.to_thread(collector.get_elasticache_clusters)

@router.get("/glacier-vaults")
async def glacier_vaults():
    return await asyncio.to_thread(collector.get_glacier_vaults)

@router.get("/backup-plans")
async def backup_plans():
    return await asyncio.to_thread(collector.get_backup_plans)

@router.get("/feature-groups")
async def sagemaker_feature_groups():
    return await asyncio.to_thread(collector.get_sagemaker_feature_group)

@router.get("/glue-databases")
async def glue_databases():
    return await asyncio.to_thread(collector.get_glue_catalog_database)

@router.get("/kinesis-streams")
async def kinesis_streams():
    return await asyncio.to_thread(collector.get_kinesis_stream)

@router.get("/msk-clusters")
async def msk_clusters():
    return await asyncio.to_thread(collector.get_msk_cluster)

# 전체 리소스를 한 번에 반환하는 API
@router.get("/all-resources")
async def all_resources():
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

    return {
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
