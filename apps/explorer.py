# apps/explorer.py
import boto3
import gzip
import json
import psycopg2

def get_s3_all_objects_content(bucket_name: str, prefix: str = "", max_keys: int = 100):
    """
    S3 버킷 안의 모든 객체 내용을 가져옴 (limit 적용)
    """
    client = boto3.client("s3", region_name="ap-northeast-2")
    paginator = client.get_paginator("list_objects_v2")
    
    results = []
    count = 0

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            if count >= max_keys:  # 안전장치
                return results

            key = obj["Key"]
            try:
                s3_obj = client.get_object(Bucket=bucket_name, Key=key)
                body = s3_obj["Body"].read()

                if key.endswith(".gz"):
                    try:
                        body = gzip.decompress(body)
                    except Exception:
                        pass

                try:
                    parsed = json.loads(body)
                except Exception:
                    try:
                        parsed = {"text": body.decode("utf-8", errors="ignore")}
                    except Exception:
                        parsed = {"raw_bytes": str(body[:200]) + "..."}
                
                results.append({
                    "key": key,
                    "size": obj["Size"],
                    "last_modified": obj["LastModified"].isoformat(),
                    "content": parsed
                })
            except Exception as e:
                results.append({"key": key, "error": str(e)})
            
            count += 1

    return results


def get_dynamodb_items(table_name: str, limit: int = 50, last_key: dict = None):
    """
    DynamoDB 테이블 아이템 조회 (limit 단위)
    """
    client = boto3.client("dynamodb", region_name="ap-northeast-2")
    params = {"TableName": table_name, "Limit": limit}
    if last_key:
        params["ExclusiveStartKey"] = last_key

    response = client.scan(**params)

    return {
        "count": response.get("Count", 0),
        "items": response.get("Items", []),
        "last_evaluated_key": response.get("LastEvaluatedKey")  # 다음 페이지 키
    }

def get_glue_data(database_name: str, table_name: str = None, max_keys: int = 20):
    """
    Glue Database에서 데이터 조회
    - table_name이 지정되면 해당 테이블 데이터만 조회
    - table_name이 없으면 모든 테이블 데이터를 조회
    """
    glue = boto3.client("glue", region_name="ap-northeast-2")
    results = []

    def fetch_table_data(tbl_name: str):
        table = glue.get_table(DatabaseName=database_name, Name=tbl_name)
        location = table["Table"]["StorageDescriptor"].get("Location")

        if not location or not location.startswith("s3://"):
            return {
                "table": tbl_name,
                "error": "지원하지 않는 저장소거나 S3 location 없음",
                "location": location
            }

        s3_path = location.replace("s3://", "")
        parts = s3_path.split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""

        objects = get_s3_all_objects_content(bucket, prefix, max_keys)
        return {
            "table": tbl_name,
            "location": location,
            "objects": objects
        }

    if table_name:
        # 특정 테이블만 조회
        return fetch_table_data(table_name)
    else:
        # 모든 테이블 조회
        paginator = glue.get_paginator("get_tables")
        for page in paginator.paginate(DatabaseName=database_name):
            for tbl in page.get("TableList", []):
                tbl_name = tbl["Name"]
                try:
                    results.append(fetch_table_data(tbl_name))
                except Exception as e:
                    results.append({
                        "table": tbl_name,
                        "error": str(e)
                    })
        return results

def get_redshift_data(endpoint: str, port: int, db_name: str, user: str, password: str, table_name: str = None, limit: int = 50):
    conn = None
    results = []

    try:
        conn = psycopg2.connect(
            host=endpoint,
            port=port,
            dbname=db_name,
            user=user,
            password=password,
            connect_timeout=10
        )
        cursor = conn.cursor()

        if not table_name:
            cursor.execute("""
                SELECT tablename
                FROM pg_table_def
                WHERE schemaname = 'public'
                GROUP BY tablename
                ORDER BY tablename;
            """)
            rows = cursor.fetchall()
            results = [{"table": r[0]} for r in rows]
        else:
            cursor.execute(f'SELECT * FROM public."{table_name}" LIMIT {limit};')
            colnames = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            results = [dict(zip(colnames, row)) for row in rows]

        cursor.close()
        return results

    except Exception as e:
        return {"error": str(e)}

    finally:
        if conn:
            conn.close()


def get_kinesis_records(stream_name: str, shard_id: str = None, limit: int = 20):
    client = boto3.client("kinesis", region_name="ap-northeast-2")

    if not shard_id:
        shards = client.describe_stream_summary(StreamName=stream_name)
        shard_response = client.describe_stream(StreamName=stream_name, Limit=1)
        shard_id = shard_response["StreamDescription"]["Shards"][0]["ShardId"]

    shard_iterator = client.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shard_id,
        ShardIteratorType="LATEST"
    )["ShardIterator"]

    response = client.get_records(ShardIterator=shard_iterator, Limit=limit)

    records = []
    for record in response.get("Records", []):
        try:
            payload = base64.b64decode(record["Data"]).decode("utf-8")
            try:
                payload = json.loads(payload)
            except Exception:
                pass
        except Exception:
            payload = str(record["Data"])

        records.append({
            "sequence_number": record["SequenceNumber"],
            "partition_key": record["PartitionKey"],
            "data": payload
        })

    return {
        "stream_name": stream_name,
        "shard_id": shard_id,
        "records": records
    }