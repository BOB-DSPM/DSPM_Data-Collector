# apps/explorer.py
from __future__ import annotations

import base64
import boto3
import gzip
import json
import os
import psycopg2
import redis
from typing import Any, Dict, List, Optional

from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError


# ──────────────────────────────────────────────────────────────────────────────
# S3: 버킷/프리픽스에서 객체 본문을 일부 수집 (최대 max_keys)
# - 버킷 미존재/권한/네트워크 등의 예외는 JSON 에러로 반환
# - 각 객체별 파싱 실패는 해당 객체 요소에 error 필드로 기록
# ──────────────────────────────────────────────────────────────────────────────
def get_s3_all_objects_content(bucket_name: str, prefix: str = "", max_keys: int = 100):
    client = boto3.client("s3", region_name=os.getenv("AWS_REGION", "ap-northeast-2"))
    paginator = client.get_paginator("list_objects_v2")

    results: List[Dict[str, Any]] = []
    count = 0

    try:
        # 버킷/프리픽스 목록 페이지네이션
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            contents = page.get("Contents", [])
            if not contents:
                # 객체가 하나도 없을 수 있음 (정상 케이스)
                continue

            for obj in contents:
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
                            # gzip이 아니거나 깨진 파일일 수 있으니 그대로 진행
                            pass

                    # JSON 시도 → 실패 시 텍스트 → 그래도 실패 시 바이트 프리뷰
                    try:
                        parsed = json.loads(body)
                    except Exception:
                        try:
                            parsed = {"text": body.decode("utf-8", errors="ignore")}
                        except Exception:
                            parsed = {"raw_bytes": (body[:200]).hex() + ("..." if len(body) > 200 else "")}

                    results.append({
                        "key": key,
                        "size": obj.get("Size"),
                        "last_modified": obj.get("LastModified").isoformat() if obj.get("LastModified") else None,
                        "content": parsed
                    })

                except ClientError as ce:
                    results.append({"key": key, "error": str(ce)})
                except Exception as e:
                    results.append({"key": key, "error": str(e)})

                count += 1

        return results

    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        # 대표적인 에러들: NoSuchBucket, AccessDenied 등
        return {
            "error": str(e),
            "bucket": bucket_name,
            "prefix": prefix,
            "code": code or "ClientError",
        }
    except (NoCredentialsError, EndpointConnectionError) as e:
        return {
            "error": str(e),
            "bucket": bucket_name,
            "prefix": prefix,
            "code": e.__class__.__name__,
        }
    except Exception as e:
        return {
            "error": str(e),
            "bucket": bucket_name,
            "prefix": prefix,
            "code": "UnknownError",
        }


# ──────────────────────────────────────────────────────────────────────────────
# DynamoDB: 간단 스캔(페이지 단위)
# ──────────────────────────────────────────────────────────────────────────────
def get_dynamodb_items(table_name: str, limit: int = 50, last_key: dict = None):
    client = boto3.client("dynamodb", region_name=os.getenv("AWS_REGION", "ap-northeast-2"))
    params = {"TableName": table_name, "Limit": limit}
    if last_key:
        params["ExclusiveStartKey"] = last_key

    response = client.scan(**params)

    return {
        "count": response.get("Count", 0),
        "items": response.get("Items", []),
        "last_evaluated_key": response.get("LastEvaluatedKey")  # 다음 페이지 키
    }


# ──────────────────────────────────────────────────────────────────────────────
# Glue: 테이블 S3 Location 따라 S3 내용 샘플링
# ──────────────────────────────────────────────────────────────────────────────
def get_glue_data(database_name: str, table_name: str = None, max_keys: int = 20):
    glue = boto3.client("glue", region_name=os.getenv("AWS_REGION", "ap-northeast-2"))
    results = []

    def fetch_table_data(tbl_name: str):
        table = glue.get_table(DatabaseName=database_name, Name=tbl_name)
        sd = table.get("Table", {}).get("StorageDescriptor", {})
        location = sd.get("Location")

        if not location or not location.startswith("s3://"):
            return {
                "table": tbl_name,
                "error": "지원하지 않는 저장소거나 S3 location 없음",
                "location": location
            }

        # s3://bucket/prefix -> bucket, prefix 분리
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
        try:
            return fetch_table_data(table_name)
        except Exception as e:
            return {"table": table_name, "error": str(e)}
    else:
        # 모든 테이블 조회
        paginator = glue.get_paginator("get_tables")
        for page in paginator.paginate(DatabaseName=database_name):
            for tbl in page.get("TableList", []):
                tbl_name = tbl.get("Name")
                try:
                    results.append(fetch_table_data(tbl_name))
                except Exception as e:
                    results.append({
                        "table": tbl_name,
                        "error": str(e)
                    })
        return results


# ──────────────────────────────────────────────────────────────────────────────
# Redshift: 간단 조회
# ──────────────────────────────────────────────────────────────────────────────
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


# ──────────────────────────────────────────────────────────────────────────────
# Kinesis: 최신 샤드에서 레코드 샘플링
# ──────────────────────────────────────────────────────────────────────────────
def get_kinesis_records(stream_name: str, shard_id: str = None, limit: int = 20):
    client = boto3.client("kinesis", region_name=os.getenv("AWS_REGION", "ap-northeast-2"))

    try:
        if not shard_id:
            # 샤드가 없을 수 있음
            desc = client.describe_stream(StreamName=stream_name)
            shards = desc.get("StreamDescription", {}).get("Shards", [])
            if not shards:
                return {"stream_name": stream_name, "error": "No shards in stream."}
            shard_id = shards[0]["ShardId"]

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
                payload = str(record.get("Data"))

            records.append({
                "sequence_number": record.get("SequenceNumber"),
                "partition_key": record.get("PartitionKey"),
                "data": payload
            })

        return {
            "stream_name": stream_name,
            "shard_id": shard_id,
            "records": records
        }

    except ClientError as e:
        return {"error": str(e), "stream_name": stream_name}
    except Exception as e:
        return {"error": str(e), "stream_name": stream_name}


# ──────────────────────────────────────────────────────────────────────────────
# SageMaker Feature Store: Offline Store(S3) 객체 샘플링
# ──────────────────────────────────────────────────────────────────────────────
def get_feature_group_data(feature_group_name: str, max_keys: int = 20):
    sm = boto3.client("sagemaker", region_name=os.getenv("AWS_REGION", "ap-northeast-2"))

    try:
        response = sm.describe_feature_group(FeatureGroupName=feature_group_name)
    except ClientError as e:
        return {"feature_group": feature_group_name, "error": str(e)}

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


# ──────────────────────────────────────────────────────────────────────────────
# RDS(Postgres): 간단 조회
# ──────────────────────────────────────────────────────────────────────────────
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


# ──────────────────────────────────────────────────────────────────────────────
# MSK(Kafka): 간단 컨슈밍 샘플
# ──────────────────────────────────────────────────────────────────────────────
def get_msk_records(cluster_arn: str, topic: str, limit: int = 20):
    client = boto3.client("kafka", region_name=os.getenv("AWS_REGION", "ap-northeast-2"))
    try:
        brokers_info = client.get_bootstrap_brokers(ClusterArn=cluster_arn)
        bootstrap_servers = brokers_info.get("BootstrapBrokerString")
        if not bootstrap_servers:
            return {"error": "Bootstrap servers not found for cluster."}
    except ClientError as e:
        return {"error": str(e)}

    from kafka import KafkaConsumer  # 지연 임포트(실행 환경 없는 경우 대비)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
        security_protocol="PLAINTEXT",
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
                "key": msg.key.decode("utf-8", errors="ignore") if msg.key else None,
                "value": payload
            })
    finally:
        consumer.close()

    return {"cluster_arn": cluster_arn, "topic": topic, "records": records}


# ──────────────────────────────────────────────────────────────────────────────
# Redis: 키/타입별 샘플링
# ──────────────────────────────────────────────────────────────────────────────
def _try_parse_bytes(data: Optional[bytes]) -> Any:
    if data is None:
        return None
    try:
        txt = data.decode("utf-8")
        try:
            return json.loads(txt)
        except Exception:
            return txt
    except Exception:
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

        _ = r.ping()  # 연결 확인

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