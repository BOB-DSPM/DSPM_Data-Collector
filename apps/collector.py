# collector.py
from sqlalchemy import create_engine
import pandas as pd
import boto3

# Steampipe PostgreSQL 연결
engine = create_engine("postgresql://steampipe@localhost:9193/steampipe")

def fetch(query: str):
    """Steampipe PostgreSQL에서 쿼리 실행 후 결과 반환"""
    df = pd.read_sql(query, engine)
    return df.to_dict(orient="records")

# ---------- AWS 리소스 조회 함수들 ----------

def get_s3_buckets():
    return fetch("""
        select name, region, creation_date
        from aws_s3_bucket
        order by region, name;
    """)

def get_ebs_volumes():
    return fetch("""
        select volume_id, size, availability_zone, encrypted,
               tags ->> 'Name' as name
        from aws_ebs_volume
        order by availability_zone, volume_id;
    """)

def get_efs_filesystems():
    return fetch("""
        select file_system_id, creation_time, size_in_bytes, region
        from aws_efs_file_system
        order by region, file_system_id;
    """)

def get_fsx_filesystems():
    return fetch("""
        select file_system_id, storage_capacity, file_system_type, lifecycle, region
        from aws_fsx_file_system
        order by region, file_system_id;
    """)

def get_rds_instances():
    return fetch("""
        select db_instance_identifier, engine, allocated_storage,
               status, endpoint_address, class
        from aws_rds_db_instance
        order by db_instance_identifier;
    """)

def get_dynamodb_tables():
    return fetch("""
        select name, table_status, read_capacity, write_capacity,
               item_count, billing_mode, region
        from aws_dynamodb_table
        order by name;
    """)

def get_redshift_clusters():
    return fetch("""
        select cluster_identifier, node_type, number_of_nodes, cluster_status,
               db_name, endpoint ->> 'address' as endpoint
        from aws_redshift_cluster
        order by cluster_identifier;
    """)

def get_rds_snapshots():
    return fetch("""
        select db_snapshot_identifier, db_instance_identifier, status, engine,
               create_time, allocated_storage, region
        from aws_rds_db_snapshot
        order by create_time desc;
    """)

def get_elasticache_clusters():
    return fetch("""
        select cache_cluster_id, engine, engine_version, cache_node_type,
               num_cache_nodes, cache_cluster_status, region
        from aws_elasticache_cluster
        order by cache_cluster_id;
    """)

def get_glacier_vaults():
    return fetch("""
        select vault_name, creation_date, vault_arn,
               number_of_archives, size_in_bytes
        from aws_glacier_vault
        order by vault_name;
    """)

def get_backup_plans():
    return fetch("""
        select name, backup_plan_id, creation_date, region
        from aws_backup_plan
        order by creation_date desc;
    """)

# ---------- boto3 API 호출 (예: SageMaker) ----------

def get_sagemaker_feature_group():
    client = boto3.client("sagemaker", region_name="ap-northeast-2")
    response = client.list_feature_groups()

    groups = {
        fg["FeatureGroupName"]: {
            "creation_time": fg["CreationTime"].isoformat(),
            "status": fg["FeatureGroupStatus"]
        }
        for fg in response.get("FeatureGroupSummaries", [])
    }
    return groups

# ---------- Steampipe로 Glue, Kinesis, MSK 조회 ----------

def get_glue_catalog_database():
    return fetch("""
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
    return fetch("""
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
    return fetch("""
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