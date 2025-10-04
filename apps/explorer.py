# apps/explorer.py
import boto3
import gzip
import json

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
