# inspector.py
from sqlalchemy import create_engine
import pandas as pd
import boto3

# Steampipe 연결
engine = create_engine("postgresql://steampipe@localhost:9193/steampipe")

def fetch(query: str):
    df = pd.read_sql(query, engine)
    return df.to_dict(orient="records")

# ---------- 상세 조회 함수 ----------

def get_s3_bucket_detail(bucket_name: str):
    return fetch(f"""
        select *
        from aws_s3_bucket
        where name = '{bucket_name}';
    """)

def get_efs_filesystem_detail(file_system_id: str):
    return fetch(f"""
        select *
        from aws_efs_file_system
        where file_system_id = '{file_system_id}';
    """)

def get_fsx_filesystem_detail(file_system_id: str):
    return fetch(f"""
        select *
        from aws_fsx_file_system
        where file_system_id = '{file_system_id}';
    """)

def get_rds_instance_detail(db_identifier: str):
    return fetch(f"""
        select *
        from aws_rds_db_instance
        where db_instance_identifier = '{db_identifier}';
    """)

def get_dynamodb_table_detail(table_name: str):
    return fetch(f"""
        select *
        from aws_dynamodb_table
        where name = '{table_name}';
    """)

def get_redshift_cluster_detail(cluster_id: str):
    return fetch(f"""
        select *
        from aws_redshift_cluster
        where cluster_identifier = '{cluster_id}';
    """)

def get_rds_snapshot_detail(snapshot_id: str):
    return fetch(f"""
        select *
        from aws_rds_db_snapshot
        where db_snapshot_identifier = '{snapshot_id}';
    """)

def get_elasticache_cluster_detail(cluster_id: str):
    return fetch(f"""
        select *
        from aws_elasticache_cluster
        where cache_cluster_id = '{cluster_id}';
    """)

def get_glacier_vault_detail(vault_name: str):
    return fetch(f"""
        select *
        from aws_glacier_vault
        where vault_name = '{vault_name}';
    """)

def get_backup_plan_detail(plan_id: str):
    return fetch(f"""
        select *
        from aws_backup_plan
        where backup_plan_id = '{plan_id}';
    """)

# ---------- boto3 API 호출 상세 ----------

def get_sagemaker_feature_group_detail(feature_group_name: str):
    client = boto3.client("sagemaker", region_name="ap-northeast-2")
    response = client.describe_feature_group(FeatureGroupName=feature_group_name)
    return response

# ---------- Steampipe 기반 기타 ----------

def get_glue_database_detail(name: str):
    return fetch(f"""
        select *
        from aws_glue_catalog_database
        where name = '{name}';
    """)

def get_kinesis_stream_detail(stream_name: str):
    return fetch(f"""
        select *
        from aws_kinesis_stream
        where stream_name = '{stream_name}';
    """)

def get_msk_cluster_detail(cluster_name: str):
    return fetch(f"""
        select *
        from aws_msk_cluster
        where cluster_name = '{cluster_name}';
    """)
