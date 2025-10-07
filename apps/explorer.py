# apps/explorer.py
import boto3
import gzip
import json
import psycopg2
from kafka import KafkaConsumer
import redis
from typing import Any, Dict, List, Optional

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

def get_feature_group_data(feature_group_name: str, max_keys: int = 20):
    sm = boto3.client("sagemaker", region_name="ap-northeast-2")
    response = sm.describe_feature_group(FeatureGroupName=feature_group_name)

    offline_store = response.get("OfflineStoreConfig", {}).get("S3StorageConfig", {})
    s3_uri = offline_store.get("ResolvedOutputS3Uri")

    if not s3_uri:
        return {"feature_group": feature_group_name, "error": "Offline Store (S3) 없음"}

    s3_path = s3_uri.replace("s3://", "")
    parts = s3_path.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""

    objects = get_s3_all_objects_content(bucket, prefix, max_keys)

    return {
        "feature_group": feature_group_name,
        "offline_store": s3_uri,
        "objects": objects
    }

def get_rds_data(endpoint: str, port: int, db_name: str, user: str, password: str, table_name: str = None, limit: int = 50):
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
                FROM pg_tables
                WHERE schemaname = 'public'
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


def get_msk_records(cluster_arn: str, topic: str, limit: int = 20):
    client = boto3.client("kafka", region_name="ap-northeast-2")
    brokers_info = client.get_bootstrap_brokers(ClusterArn=cluster_arn)
    bootstrap_servers = brokers_info.get("BootstrapBrokerString")

    if not bootstrap_servers:
        return {"error": "Bootstrap servers not found for cluster."}

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="latest",  
        enable_auto_commit=False,
        consumer_timeout_ms=5000,   
        security_protocol="PLAINTEXT"
    )

    records = []
    try:
        for i, msg in enumerate(consumer):
            if i >= limit:
                break
            try:
                payload = msg.value.decode("utf-8")
                try:
                    payload = json.loads(payload)
                except Exception:
                    pass
            except Exception:
                payload = str(msg.value)

            records.append({
                "topic": msg.topic,
                "partition": msg.partition,
                "offset": msg.offset,
                "key": msg.key.decode("utf-8") if msg.key else None,
                "value": payload
            })
    finally:
        consumer.close()

    return {"cluster_arn": cluster_arn, "topic": topic, "records": records}

def _try_parse_bytes(data: Optional[bytes]) -> Any:
    if data is None:
        return None
    # 바이트 → 텍스트/JSON 추론
    try:
        txt = data.decode("utf-8")
        try:
            return json.loads(txt)
        except Exception:
            return txt
    except Exception:
        # 사람이 볼 수 있도록 앞부분만
        return {"raw_bytes_preview": str(data[:200]) + ("..." if len(data) > 200 else "")}

def get_redis_data(
    host: str,
    port: int = 6379,
    password: Optional[str] = None,
    db: int = 0,
    pattern: str = "*",
    limit: int = 50,
    per_collection_limit: int = 50,
) -> Dict[str, Any]:
    r = None
    try:
        r = redis.Redis(
            host=host,
            port=port,
            password=password,
            db=db,
            socket_timeout=5,          
            socket_connect_timeout=5,
            decode_responses=False,   
        )

        pong = r.ping()

        results: List[Dict[str, Any]] = []
        scanned = 0
        cursor = 0

        while True:
            cursor, keys = r.scan(cursor=cursor, match=pattern, count=200)
            for key in keys:
                if scanned >= limit:
                    return {
                        "host": host,
                        "port": port,
                        "db": db,
                        "pattern": pattern,
                        "keys_returned": len(results),
                        "keys_limit": limit,
                        "items": results,
                    }

                k = key 
                k_str = k.decode("utf-8", errors="ignore")

                try:
                    ktype = r.type(k) 
                    ktype_str = ktype.decode("utf-8")

                    ttl = r.ttl(k) 
                    try:
                        mem = r.memory_usage(k)
                    except Exception:
                        mem = None

                    item: Dict[str, Any] = {
                        "key": k_str,
                        "type": ktype_str,
                        "ttl": ttl,
                        "memory_usage": mem,
                    }

                    if ktype_str == "string":
                        val = r.get(k)
                        item["value"] = _try_parse_bytes(val)

                    elif ktype_str == "list":
                        vals = r.lrange(k, 0, max(per_collection_limit - 1, 0))
                        item["values"] = [_try_parse_bytes(v) for v in vals]
                        item["length"] = r.llen(k)

                    elif ktype_str == "set":
                        scursor = 0
                        svals: List[Any] = []
                        while True:
                            scursor, members = r.sscan(k, scursor, count=per_collection_limit)
                            svals.extend(members)
                            if scursor == 0 or len(svals) >= per_collection_limit:
                                break
                        item["values"] = [_try_parse_bytes(v) for v in svals[:per_collection_limit]]
                        try:
                            item["length"] = r.scard(k)
                        except Exception:
                            pass

                    elif ktype_str == "zset":
                        vals = r.zrange(k, 0, max(per_collection_limit - 1, 0), withscores=True)
                        item["values"] = [
                            {"member": _try_parse_bytes(m), "score": s} for (m, s) in vals
                        ]
                        try:
                            item["length"] = r.zcard(k)
                        except Exception:
                            pass

                    elif ktype_str == "hash":
                        hcursor = 0
                        hitems: List[Dict[str, Any]] = []
                        while True:
                            hcursor, pairs = r.hscan(k, hcursor, count=per_collection_limit)
                            for field, val in pairs.items():
                                hitems.append({
                                    "field": field.decode("utf-8", errors="ignore"),
                                    "value": _try_parse_bytes(val),
                                })
                            if hcursor == 0 or len(hitems) >= per_collection_limit:
                                break
                        item["items"] = hitems[:per_collection_limit]
                        try:
                            item["length"] = r.hlen(k)
                        except Exception:
                            pass

                    elif ktype_str == "stream":
                        entries = r.xrevrange(k, count=per_collection_limit)
                        parsed = []
                        for entry_id, fields in entries:
                            parsed.append({
                                "id": entry_id.decode("utf-8", errors="ignore"),
                                "fields": {
                                    (fk.decode("utf-8", errors="ignore")): _try_parse_bytes(fv)
                                    for fk, fv in fields.items()
                                }
                            })
                        item["entries"] = parsed

                    else:
                        item["note"] = "Unsupported or module type (value sampling skipped)."

                except Exception as e_key:
                    item = {
                        "key": k_str,
                        "error": str(e_key),
                    }

                results.append(item)
                scanned += 1

            if cursor == 0:
                break

        return {
            "host": host,
            "port": port,
            "db": db,
            "pattern": pattern,
            "keys_returned": len(results),
            "keys_limit": limit,
            "items": results,
        }

    except Exception as e:
        return {"error": str(e), "host": host, "port": port, "db": db, "pattern": pattern}
    finally:
        if r:
            try:
                r.close()
            except Exception:
                pass