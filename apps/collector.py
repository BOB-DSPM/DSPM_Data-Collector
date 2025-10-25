# collector.py
from sqlalchemy import create_engine, text
import pandas as pd
import boto3
import os

# Steampipe PostgreSQL 연결
engine = create_engine("postgresql://steampipe@localhost:9193/steampipe")

def fetch(query: str):
    """Steampipe PostgreSQL에서 쿼리 실행 후 결과 반환"""
    # SQLAlchemy 2.x + pandas 호환: text() 래핑 + 명시적 커넥션 + 빈 params 튜플
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

ALLOWED_REGIONS = get_opted_in_regions() or ["ap-northeast-2"]  # 안전 기본값

def region_in_clause(alias: str = "") -> str:
    """
    region 컬럼이 있는 테이블에서 쓰는 WHERE 절 스니펫.
    alias가 있으면 'alias.region in (...)' 형태로 만든다.
    """
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

# ---------- AWS 리소스 조회 함수들 (opt-in 필터 적용) ----------

def get_s3_buckets():
    return fetch(f"""
        select name, region, creation_date
        from aws_s3_bucket
        where {region_in_clause()}
        order by region, name;
    """)

def get_ebs_volumes():
    return fetch(f"""
        select
          volume_id,
          size,
          availability_zone,
          encrypted,
          tags ->> 'Name' as name
        from aws_ebs_volume
        where {az_matches_allowed()}
        order by availability_zone, volume_id;
    """)

def get_efs_filesystems():
    # size_in_bytes 는 JSON 이라 value 추출
    return fetch(f"""
        select
          file_system_id,
          creation_time,
          coalesce((size_in_bytes ->> 'value')::bigint, 0) as size_bytes,
          region
        from aws_efs_file_system
        where {region_in_clause()}
        order by region, file_system_id;
    """)

def get_fsx_filesystems():
    return fetch(f"""
        select file_system_id, storage_capacity, file_system_type, lifecycle, region
        from aws_fsx_file_system
        where {region_in_clause()}
        order by region, file_system_id;
    """)

def get_rds_instances():
    # 컬럼명 정정: db_instance_class / db_instance_status / endpoint ->> 'address'
    return fetch(f"""
        select
          db_instance_identifier,
          engine,
          allocated_storage,
          db_instance_status as status,
          (endpoint ->> 'address') as endpoint_address,
          db_instance_class as class,
          region
        from aws_rds_db_instance
        where {region_in_clause()}
        order by db_instance_identifier;
    """)

def get_dynamodb_tables():
    # 일부 배포에서 read_capacity/write_capacity 컬럼이 없어 실패할 수 있어 최소 컬럼만 사용
    return fetch(f"""
        select
          name,
          table_status,
          item_count,
          billing_mode,
          region
        from aws_dynamodb_table
        where {region_in_clause()}
        order by name;
    """)

def get_redshift_clusters():
    return fetch(f"""
        select
          cluster_identifier,
          node_type,
          number_of_nodes,
          cluster_status,
          db_name,
          endpoint ->> 'address' as endpoint,
          region
        from aws_redshift_cluster
        where {region_in_clause()}
        order by cluster_identifier;
    """)

def get_rds_snapshots():
    return fetch(f"""
        select
          db_snapshot_identifier,
          db_instance_identifier,
          status,
          engine,
          create_time,
          allocated_storage,
          region
        from aws_rds_db_snapshot
        where {region_in_clause()}
        order by create_time desc;
    """)

def get_elasticache_clusters():
    return fetch(f"""
        select
          cache_cluster_id,
          engine,
          engine_version,
          cache_node_type,
          num_cache_nodes,
          cache_cluster_status,
          region
        from aws_elasticache_cluster
        where {region_in_clause()}
        order by cache_cluster_id;
    """)

def get_glacier_vaults():
    return fetch(f"""
        select
          vault_name,
          creation_date,
          vault_arn,
          number_of_archives,
          size_in_bytes,
          region
        from aws_glacier_vault
        where {region_in_clause()}
        order by vault_name;
    """)

def get_backup_plans():
    return fetch(f"""
        select name, backup_plan_id, creation_date, region
        from aws_backup_plan
        where {region_in_clause()}
        order by creation_date desc;
    """)

# ---------- boto3 API (예: SageMaker) ----------
def get_sagemaker_feature_group():
    # boto3는 코드에 리전 고정 or ALLOWED_REGIONS 첫 번째 사용
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
        select
          name,
          description,
          location_uri,
          create_time,
          catalog_id,
          region
        from aws_glue_catalog_database
        where {region_in_clause()}
        order by name;
    """)

def get_kinesis_stream():
    return fetch(f"""
        select
          stream_name,
          stream_arn,
          stream_status,
          open_shard_count,
          region
        from aws_kinesis_stream
        where {region_in_clause()}
        order by stream_name;
    """)

def get_msk_cluster():
    return fetch(f"""
        select
          cluster_name,
          arn,
          state,
          current_version as kafka_version,
          region
        from aws_msk_cluster
        where {region_in_clause()}
        order by cluster_name;
    """)
