from fastapi import FastAPI
from sqlalchemy import create_engine
import pandas as pd

app = FastAPI()

# Steampipe PostgreSQL 연결
engine = create_engine("postgresql://steampipe@localhost:9193/steampipe")

@app.get("/s3-buckets")
def get_s3_buckets():
    query = """
    select
      name,
      region,
      creation_date
    from
      aws_s3_bucket
    order by
      region, name;
    """
    df = pd.read_sql(query, engine)
    return df.to_dict(orient="records")

@app.get("/ebs-volumes")
def get_ebs_volume():
    query = """
        select
        volume_id,
        size,
        availability_zone,
        encrypted,
        tags ->> 'Name' as name
        from
        aws_ebs_volume
        order by
        availability_zone, volume_id;
    """
    df = pd.read_sql(query, engine)
    return df.to_dict(orient="records")

@app.get("/efs-filesystems")
def get_efs_filesystems():
    query = """
    select
      file_system_id,
      creation_time,
      size_in_bytes,
      region
    from
      aws_efs_file_system
    order by
      region, file_system_id;
    """
    df = pd.read_sql(query, engine)
    return df.to_dict(orient="records")

@app.get("/fsx-filesystems")
def get_fsx_filesystems():
    query = """
        select
        file_system_id,
        storage_capacity,
        file_system_type,
        lifecycle,
        region
        from
        aws_fsx_file_system
        order by
        region, file_system_id;

    """
    df = pd.read_sql(query, engine)
    return df.to_dict(orient="records")

@app.get("/rds-instances")
def get_rds_instances():
    query = """
        select
        db_instance_identifier,
        engine,
        allocated_storage,
        status,
        endpoint_address,
        class
        from
        aws_rds_db_instance
        order by
        db_instance_identifier;
    """
    df = pd.read_sql(query, engine)
    return df.to_dict(orient="records")

@app.get("/dynamodb-tables")
def get_dynamodb_tables():
    query = """
    select
      name,
      table_status,
      read_capacity,
      write_capacity,
      item_count,
      billing_mode,
      region
    from
      aws_dynamodb_table
    order by
      name;
    """
    df = pd.read_sql(query, engine)
    return df.to_dict(orient="records")

@app.get("/redshift-clusters")
def get_redshift_clusters():
    query = """
    select
      cluster_identifier,
      node_type,
      number_of_nodes,
      cluster_status,
      db_name,
      endpoint ->> 'address' as endpoint
    from
      aws_redshift_cluster
    order by
      cluster_identifier;
    """
    df = pd.read_sql(query, engine)
    return df.to_dict(orient="records")

@app.get("/rds-snapshots")
def get_rds_snapshots():
    query = """
    select
      db_snapshot_identifier,
      db_instance_identifier,
      status,
      engine,
      create_time,
      allocated_storage,
      region
    from
      aws_rds_db_snapshot
    order by
      create_time desc;
    """
    df = pd.read_sql(query, engine)
    return df.to_dict(orient="records")

@app.get("/elasticache-clusters")
def get_elasticache_clusters():
    query = """
    select
      cache_cluster_id,
      engine,
      engine_version,
      cache_node_type,
      num_cache_nodes,
      cache_cluster_status,
      region
    from
      aws_elasticache_cluster
    order by
      cache_cluster_id;
    """
    df = pd.read_sql(query, engine)
    return df.to_dict(orient="records")

@app.get("/glacier-vaults")
def get_glacier_vaults():
    query = """
    select
      vault_name,
      creation_date,
      vault_arn,
      number_of_archives,
      size_in_bytes
    from
      aws_glacier_vault
    order by
      vault_name;
    """
    df = pd.read_sql(query, engine)
    return df.to_dict(orient="records")

@app.get("/backup-plans")
def get_backup_plans():
    query = """
    select
      name,
      backup_plan_id,
      creation_date,
      region
    from
      aws_backup_plan
    order by
      creation_date desc;
    """
    df = pd.read_sql(query, engine)
    return df.to_dict(orient="records")
