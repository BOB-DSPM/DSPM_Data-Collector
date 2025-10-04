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
