# collector.py
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
import numpy as np
import math
import pandas as pd
import boto3
import os
import logging

# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------
logger = logging.getLogger("collector")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

# ------------------------------------------------------------
# Steampipe PostgreSQL 연결
# ------------------------------------------------------------
def _build_steampipe_url() -> str:
    url = os.getenv("STEAMPIPE_DB_URL")
    if url:
        return url

    user = os.getenv("STEAMPIPE_DB_USER", "steampipe")
    password = os.getenv("STEAMPIPE_DB_PASSWORD", "")
    host = os.getenv("STEAMPIPE_DB_HOST", "localhost")
    port = os.getenv("STEAMPIPE_DB_PORT", "9193")
    name = os.getenv("STEAMPIPE_DB_NAME", "steampipe")
    credentials = f"{user}:{password}" if password else user
    return f"postgresql://{credentials}@{host}:{port}/{name}"

engine = create_engine(_build_steampipe_url())

def fetch(query: str):
    """
    Steampipe PostgreSQL에서 쿼리 실행 후 결과 반환.
    OptIn/권한 오류는 건너뛰고 빈 리스트 반환하여 API가 500으로 터지지 않도록 방어.
    """
    SKIP_MARKERS = (
        "OptInRequired",
        "SubscriptionRequiredException",
        "AccessDenied",
        "UnauthorizedOperation",
        "AuthFailure",
        "ExpiredToken",
        "AccessDeniedException",
        "Throttling",  # 혹시 모를 과금/제한
    )
    try:
        with engine.connect() as conn:
            try:
                df = pd.read_sql_query(text(query), conn, params=())
                # pandas -> dict 변환 시 NaN/NaT/inf가 남으면 JSON 직렬화에서 ValueError 발생하므로 None으로 치환
                df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
                df = df.where(pd.notnull(df), None)
                data = df.to_dict(orient="records")

                def _sanitize(val):
                    if isinstance(val, float):
                        if math.isnan(val) or math.isinf(val):
                            return None
                        return val
                    if isinstance(val, list):
                        return [_sanitize(v) for v in val]
                    if isinstance(val, dict):
                        return {k: _sanitize(v) for k, v in val.items()}
                    return val

                return [_sanitize(row) for row in data]
            except Exception as e:
                msg = str(e)
                if any(m in msg for m in SKIP_MARKERS):
                    logger.warning(f"Steampipe query skipped (opt-in/permission): {msg.splitlines()[0]}")
                    return []
                # 알 수 없는 예외는 그대로 올림(디버그 필요)
                raise
    except OperationalError as e:
        logger.error(f"Steampipe connection failed: {e}")
        return []

# ------------------------------------------------------------
# opt-in 리전 로딩 & 필터 헬퍼
# ------------------------------------------------------------
def get_opted_in_regions():
    """aws_region에서 opt-in된 리전만 로딩 (환경변수 ALLOWED_REGIONS 우선)"""
    env = os.getenv("ALLOWED_REGIONS")
    if env:
        regions = [r.strip() for r in env.split(",") if r.strip()]
        return regions

    try:
        rows = fetch("""
            select region
            from aws_region
            where opt_in_status in ('opted-in', 'opt-in-not-required')
        """)
        return [r["region"] for r in rows]
    except Exception as e:
        # 연결 실패 시 서비스가 부팅조차 되지 않는 문제를 막기 위한 안전 장치
        fallback = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-northeast-2"
        logger.warning(f"Steampipe region lookup failed: {e}. Falling back to [{fallback}]")
        return [fallback]

ALLOWED_REGIONS = get_opted_in_regions() or ["ap-northeast-2"]  # 안전 기본값

DEFAULT_BOTO_REGION = (
    os.getenv("AWS_REGION")
    or os.getenv("AWS_DEFAULT_REGION")
    or (ALLOWED_REGIONS[0] if ALLOWED_REGIONS else "ap-northeast-2")
)

def region_in_clause(alias: str = "") -> str:
    """region 컬럼이 있는 테이블에서 쓰는 WHERE 절 스니펫."""
    col = f"{alias}.region" if alias else "region"
    quoted = ", ".join(f"'{r}'" for r in ALLOWED_REGIONS)
    return f"{col} in ({quoted})"

def az_matches_allowed(alias: str = "") -> str:
    """
    availability_zone만 있는 테이블(EBS 등) 필터.
    '<region>%’ 패턴과 매칭.
    """
    col = f"{alias}.availability_zone" if alias else "availability_zone"
    values_rows = ", ".join(f"('{r}')" for r in ALLOWED_REGIONS)
    return f"""exists (
        select 1
        from (values {values_rows}) as v(region)
        where {col} like v.region || '%'
    )"""

# ------------------------------------------------------------
# AWS 리소스 조회 함수들 (select * + opt-in 필터)
# ------------------------------------------------------------
def get_s3_buckets():
    return fetch(f"""
        select *
        from aws_s3_bucket
        where {region_in_clause()}
        order by 1;
    """)

def get_ebs_volumes():
    return fetch(f"""
        select *
        from aws_ebs_volume
        where {az_matches_allowed()}
        order by 1;
    """)

def get_efs_filesystems():
    return fetch(f"""
        select *
        from aws_efs_file_system
        where {region_in_clause()}
        order by 1;
    """)

def get_fsx_filesystems():
    return fetch(f"""
        select *
        from aws_fsx_file_system
        where {region_in_clause()}
        order by 1;
    """)

def get_rds_instances():
    return fetch(f"""
        select *
        from aws_rds_db_instance
        where {region_in_clause()}
        order by 1;
    """)

def get_dynamodb_tables():
    return fetch(f"""
        select *
        from aws_dynamodb_table
        where {region_in_clause()}
        order by 1;
    """)

def get_redshift_clusters():
    return fetch(f"""
        select *
        from aws_redshift_cluster
        where {region_in_clause()}
        order by 1;
    """)

def get_rds_snapshots():
    return fetch(f"""
        select *
        from aws_rds_db_snapshot
        where {region_in_clause()}
        order by 1;
    """)

def get_elasticache_clusters():
    return fetch(f"""
        select *
        from aws_elasticache_cluster
        where {region_in_clause()}
        order by 1;
    """)

def get_glacier_vaults():
    return fetch(f"""
        select *
        from aws_glacier_vault
        where {region_in_clause()}
        order by 1;
    """)

def get_backup_plans():
    return fetch(f"""
        select *
        from aws_backup_plan
        where {region_in_clause()}
        order by 1;
    """)

def get_glue_catalog_database():
    return fetch(f"""
        select *
        from aws_glue_catalog_database
        where {region_in_clause()}
        order by 1;
    """)

def get_kinesis_stream():
    return fetch(f"""
        select *
        from aws_kinesis_stream
        where {region_in_clause()}
        order by 1;
    """)

def get_msk_cluster():
    return fetch(f"""
        select *
        from aws_msk_cluster
        where {region_in_clause()}
        order by 1;
    """)

# ------------------------------------------------------------
# boto3 API (예: SageMaker)
# ------------------------------------------------------------
def get_sagemaker_feature_group():
    # boto3는 코드에 리전 고정 or ALLOWED_REGIONS 첫 번째 사용
    client = boto3.client("sagemaker", region_name=DEFAULT_BOTO_REGION)
    resp = client.list_feature_groups()
    return {
        fg["FeatureGroupName"]: {
            "creation_time": fg["CreationTime"].isoformat(),
            "status": fg["FeatureGroupStatus"]
        }
        for fg in resp.get("FeatureGroupSummaries", [])
    }
