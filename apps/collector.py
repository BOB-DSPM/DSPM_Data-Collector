# collector.py
import os
import logging
from typing import List, Dict, Any

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, ProgrammingError, SQLAlchemyError
import boto3
from botocore.exceptions import BotoCoreError, ClientError

# ----------------------------------
# 로깅
# ----------------------------------
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

# ----------------------------------
# Steampipe PostgreSQL 연결
# ----------------------------------
STEAMPIPE_PG_URL = os.getenv(
    "STEAMPIPE_PG_URL",
    "postgresql://steampipe@localhost:9193/steampipe"
)
engine = create_engine(STEAMPIPE_PG_URL)


def _is_optin_or_access_error(err: Exception) -> bool:
    """
    AWS 미가입(OptInRequired/SubscriptionRequiredException), 권한 부족, 403 등을 판별.
    에러 메시지 문자열을 기반으로 가볍게 필터링.
    """
    s = str(err).lower()
    keywords = [
        "optinrequired",
        "subscriptionrequiredexception",
        "the aws access key id needs a subscription",
        "need a subscription",
        "accessdenied",
        "unauthorized",
        "not authorized",
        "forbidden",
        "authorization",
        "signaturedoesnotmatch",
        "403",  # 보조 신호
    ]
    return any(k in s for k in keywords)


def _safe_fetch(query: str) -> List[Dict[str, Any]]:
    """
    Steampipe PostgreSQL에서 쿼리 실행.
    실패 시(미가입/권한/네트워크/서비스 비가용) 빈 리스트 반환 + 로그.
    """
    try:
        df = pd.read_sql(query, engine)
        return df.to_dict(orient="records")
    except (OperationalError, ProgrammingError) as e:
        if _is_optin_or_access_error(e):
            logger.warning("Steampipe query skipped (opt-in/permission): %s", e)
            return []
        logger.error("Steampipe query failed: %s", e)
        return []
    except SQLAlchemyError as e:
        logger.error("SQLAlchemy error: %s", e)
        return []
    except Exception as e:
        logger.exception("Unexpected error in fetch(): %s", e)
        return []


# ---------- AWS 리소스 조회 함수들 (Steampipe) ----------
def get_s3_buckets():
    return _safe_fetch("""
        select name, region, creation_date
        from aws_s3_bucket
        order by region, name;
    """)

def get_ebs_volumes():
    return _safe_fetch("""
        select volume_id, size, availability_zone, encrypted,
               tags ->> 'Name' as name
        from aws_ebs_volume
        order by availability_zone, volume_id;
    """)

def get_efs_filesystems():
    return _safe_fetch("""
        select file_system_id, creation_time, size_in_bytes, region
        from aws_efs_file_system
        order by region, file_system_id;
    """)

def get_fsx_filesystems():
    return _safe_fetch("""
        select file_system_id, storage_capacity, file_system_type, lifecycle, region
        from aws_fsx_file_system
        order by region, file_system_id;
    """)

def get_rds_instances():
    return _safe_fetch("""
        select db_instance_identifier, engine, allocated_storage,
               status, endpoint_address, class
        from aws_rds_db_instance
        order by db_instance_identifier;
    """)

def get_dynamodb_tables():
    return _safe_fetch("""
        select name, table_status, read_capacity, write_capacity,
               item_count, billing_mode, region
        from aws_dynamodb_table
        order by name;
    """)

def get_redshift_clusters():
    return _safe_fetch("""
        select cluster_identifier, node_type, number_of_nodes, cluster_status,
               db_name, endpoint ->> 'address' as endpoint
        from aws_redshift_cluster
        order by cluster_identifier;
    """)

def get_rds_snapshots():
    return _safe_fetch("""
        select db_snapshot_identifier, db_instance_identifier, status, engine,
               create_time, allocated_storage, region
        from aws_rds_db_snapshot
        order by create_time desc;
    """)

def get_elasticache_clusters():
    return _safe_fetch("""
        select cache_cluster_id, engine, engine_version, cache_node_type,
               num_cache_nodes, cache_cluster_status, region
        from aws_elasticache_cluster
        order by cache_cluster_id;
    """)

def get_glacier_vaults():
    return _safe_fetch("""
        select vault_name, creation_date, vault_arn,
               number_of_archives, size_in_bytes
        from aws_glacier_vault
        order by vault_name;
    """)

def get_backup_plans():
    return _safe_fetch("""
        select name, backup_plan_id, creation_date, region
        from aws_backup_plan
        order by creation_date desc;
    """)

# ---------- boto3 API 호출 (예: SageMaker) ----------
def get_sagemaker_feature_group():
    """
    dict 반환. 실패 시 빈 dict.
    """
    try:
        region = os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2")
        client = boto3.client("sagemaker", region_name=region)
        response = client.list_feature_groups()
        groups = {
            fg["FeatureGroupName"]: {
                "creation_time": fg["CreationTime"].isoformat(),
                "status": fg["FeatureGroupStatus"]
            }
            for fg in response.get("FeatureGroupSummaries", [])
        }
        return groups
    except (ClientError, BotoCoreError) as e:
        if _is_optin_or_access_error(e):
            logger.warning("SageMaker not available/unauthorized: %s", e)
            return {}
        logger.error("boto3 error: %s", e)
        return {}
    except Exception as e:
        logger.exception("Unexpected boto3 error: %s", e)
        return {}

# ---------- Steampipe로 Glue, Kinesis, MSK 조회 ----------
def get_glue_catalog_database():
    return _safe_fetch("""
        select
          name,
          description,
          location_uri,
          create_time,
          catalog_id,
          region
        from
          aws_glue_catalog_database
        order by
          name;
    """)

def get_kinesis_stream():
    return _safe_fetch("""
        select
          stream_name,
          stream_arn,
          stream_status,
          open_shard_count,
          region
        from
          aws_kinesis_stream
        order by
          stream_name;
    """)

def get_msk_cluster():
    return _safe_fetch("""
        select
          cluster_name,
          arn,
          state,
          current_version as kafka_version,
          region
        from
          aws_msk_cluster
        order by
          cluster_name;
    """)
