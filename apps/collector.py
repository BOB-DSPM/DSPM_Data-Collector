# collector.py
from sqlalchemy import create_engine, text
import pandas as pd
import boto3
import os

# Steampipe PostgreSQL 연결
engine = create_engine("postgresql://steampipe@localhost:9193/steampipe")

def fetch(query: str):
    """Steampipe PostgreSQL에서 쿼리 실행 후 결과 반환"""
    with engine.connect() as conn:
        df = pd.read_sql_query(text(query), conn, params=())
    return df.to_dict(orient="records")

# --- opt-in 리전 로딩 & 필터 헬퍼 ---

def get_opted_in_regions():
    """aws_region에서 opt-in된 리전만 로딩 (환경변수 ALLOWED_REGIONS 우선)"""
    env = os.getenv("ALLOWED_REGIONS")
    if env:
        regions = [r.strip() for r in env.split(",") if r.strip()]
        return regions

    rows = fetch("""
        select region
        from aws_region
        where opt_in_status in ('opted-in', 'opt-in-not-required')
    """)
    return [r["region"] for r in rows]

ALLOWED_REGIONS = get_opted_in_regions() or ["ap-northeast-2"]

def region_in_clause(alias: str = "") -> str:
    col = f"{alias}.region" if alias else "region"
    quoted = ", ".join(f"'{r}'" for r in ALLOWED_REGIONS)
    return f"{col} in ({quoted})"

def az_matches_allowed(alias: str = "") -> str:
    col = f"{alias}.availability_zone" if alias else "availability_zone"
    values_rows = ", ".join(f"('{r}')" for r in ALLOWED_REGIONS)
    return f"""exists (
        select 1
        from (values {values_rows}) as v(region)
        where {col} like v.region || '%'
    )"""

# ---------- AWS 리소스 조회 함수들 (select * + opt-in 필터) ----------

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

# ---------- boto3 API (예: SageMaker) ----------
def get_sagemaker_feature_group():
    region = os.getenv("AWS_REGION") or (ALLOWED_REGIONS[0] if ALLOWED_REGIONS else "ap-northeast-2")
    client = boto3.client("sagemaker", region_name=region)
    resp = client.list_feature_groups()
    return {
        fg["FeatureGroupName"]: {
            "creation_time": fg["CreationTime"].isoformat(),
            "status": fg["FeatureGroupStatus"]
        }
        for fg in resp.get("FeatureGroupSummaries", [])
    }

# ---------- Steampipe로 Glue, Kinesis, MSK 조회 ----------
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
