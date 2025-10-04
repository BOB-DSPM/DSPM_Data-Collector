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