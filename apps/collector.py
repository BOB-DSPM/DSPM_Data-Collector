# collector.py (hardened)
import os
import time
import logging
from typing import List, Dict, Any, Iterable, Tuple, Optional, Callable
from functools import wraps

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import (
    OperationalError,
    ProgrammingError,
    SQLAlchemyError,
    InterfaceError,
    DBAPIError,
)
import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError

# ----------------------------------
# 로깅
# ----------------------------------
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

# ----------------------------------
# Steampipe PostgreSQL 연결
#  - pool_pre_ping=True 로 커넥션 생존 체크
#  - env로 타임아웃 제어(지원할 경우)
# ----------------------------------
STEAMPIPE_PG_URL = os.getenv(
    "STEAMPIPE_PG_URL",
    "postgresql://steampipe@localhost:9193/steampipe",
)
STATEMENT_TIMEOUT_MS = int(os.getenv("STEAMPIPE_STATEMENT_TIMEOUT_MS", "60000"))  # 60s
# Postgres의 statement_timeout을 적용할 수 있는 경우를 대비해 options 파라미터 사용 (미지원이어도 무해)
ENGINE_OPTIONS = {
    "pool_pre_ping": True,
}
if "options=" not in STEAMPIPE_PG_URL:
    STEAMPIPE_PG_URL = (
        f"{STEAMPIPE_PG_URL}?options=-c statement_timeout={STATEMENT_TIMEOUT_MS}"
    )

engine = create_engine(STEAMPIPE_PG_URL, **ENGINE_OPTIONS)


# ----------------------------------
# 공통: 재시도 데코레이터 (지수 백오프)
# ----------------------------------
def retry(
    exceptions: Tuple[type, ...],
    tries: int = 3,
    delay: float = 0.5,
    backoff: float = 2.0,
    jitter: float = 0.1,
    on_retry: Optional[Callable[[Exception, int], None]] = None,
):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            _tries, _delay = tries, delay
            last_exc: Optional[Exception] = None
            while _tries > 0:
                try:
                    return fn(*args, **kwargs)
                except exceptions as e:
                    last_exc = e
                    _tries -= 1
                    if _tries <= 0:
                        break
                    if on_retry:
                        on_retry(e, _tries)
                    time.sleep(_delay)
                    _delay = _delay * backoff + jitter
            # 재시도 소진
            raise last_exc  # type: ignore[misc]
        return wrapper
    return decorator


# ----------------------------------
# 에러 카테고리 판별
# ----------------------------------
def _is_optin_or_access_error(err: Exception) -> bool:
    """
    AWS 미가입(OptInRequired/SubscriptionRequiredException), 권한 부족, 403 등을 판별.
    에러 메시지 문자열 기반 (Steampipe FDW, boto3 공통).
    """
    s = str(err).lower()
    keywords = [
        "optinrequired",
        "subscriptionrequiredexception",
        "the aws access key id needs a subscription",
        "need a subscription",
        "accessdenied",
        "access denied",
        "unauthorized",
        "not authorized",
        "forbidden",
        "authorization",
        "signaturedoesnotmatch",
        "403",
    ]
    return any(k in s for k in keywords)


def _is_transient_sql_error(err: Exception) -> bool:
    """
    일시적 네트워크/커넥션/서버 부하류 판단(재시도 대상).
    """
    s = str(err).lower()
    hints = [
        "connection refused",
        "server closed the connection",
        "timeout",
        "timed out",
        "could not connect",
        "connection reset",
        "too many connections",
        "canceling statement due to statement timeout",
        "deadlock",
        "try again",
    ]
    return any(h in s for h in hints)


# ----------------------------------
# 안전한 Steampipe 쿼리
#  - 일시 오류 재시도
#  - 권한/opt-in 문제는 경고 후 빈 결과
#  - 예외는 로깅 후 빈 결과
# ----------------------------------
@retry(
    exceptions=(OperationalError, InterfaceError, DBAPIError),
    tries=int(os.getenv("STEAMPIPE_RETRIES", "3")),
    delay=float(os.getenv("STEAMPIPE_RETRY_DELAY", "0.5")),
    backoff=float(os.getenv("STEAMPIPE_RETRY_BACKOFF", "2.0")),
    jitter=float(os.getenv("STEAMPIPE_RETRY_JITTER", "0.1")),
    on_retry=lambda e, left: logger.warning(
        "Steampipe transient error, retrying... (%s retries left) err=%s", left, e
    ),
)
def _fetch_df(query: str) -> pd.DataFrame:
    # 쿼리는 text()로 래핑 (SQLAlchemy 2.x 호환)
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn)


def _safe_fetch(query: str) -> List[Dict[str, Any]]:
    """
    Steampipe PostgreSQL에서 쿼리 실행.
    실패 시(미가입/권한/네트워크/서비스 비가용) 빈 리스트 반환 + 로그.
    """
    try:
        df = _fetch_df(query)
        return df.to_dict(orient="records")
    except (OperationalError, ProgrammingError, InterfaceError, DBAPIError) as e:
        # 권한/opt-in 문제는 경고 + 빈 결과
        if _is_optin_or_access_error(e):
            logger.warning("Steampipe query skipped (opt-in/permission): %s", e)
            return []
        # 일시적 오류인데 재시도 끝나고 도달 → 에러로 기록, 엔진 재생성 시도
        if _is_transient_sql_error(e):
            logger.error("Steampipe transient error (retries exhausted): %s", e)
            try:
                engine.dispose()
            except Exception:
                pass
            return []
        logger.error("Steampipe query failed: %s", e)
        return []
    except SQLAlchemyError as e:
        logger.error("SQLAlchemy error: %s", e)
        return []
    except Exception as e:
        logger.exception("Unexpected error in fetch(): %s", e)
        return []


# ---------- AWS 리소스 조회 함수들 (Steampipe) ----------
def get_s3_buckets():
    return _safe_fetch("""
        select name, region, creation_date
        from aws_s3_bucket
        order by region, name;
    """)

def get_ebs_volumes():
    return _safe_fetch("""
        select volume_id, size, availability_zone, encrypted,
               tags ->> 'Name' as name
        from aws_ebs_volume
        order by availability_zone, volume_id;
    """)

def get_efs_filesystems():
    return _safe_fetch("""
        select file_system_id, creation_time, size_in_bytes, region
        from aws_efs_file_system
        order by region, file_system_id;
    """)

def get_fsx_filesystems():
    return _safe_fetch("""
        select file_system_id, storage_capacity, file_system_type, lifecycle, region
        from aws_fsx_file_system
        order by region, file_system_id;
    """)

def get_rds_instances():
    return _safe_fetch("""
        select db_instance_identifier, engine, allocated_storage,
               status, endpoint_address, class
        from aws_rds_db_instance
        order by db_instance_identifier;
    """)

def get_dynamodb_tables():
    return _safe_fetch("""
        select name, table_status, read_capacity, write_capacity,
               item_count, billing_mode, region
        from aws_dynamodb_table
        order by name;
    """)

def get_redshift_clusters():
    return _safe_fetch("""
        select cluster_identifier, node_type, number_of_nodes, cluster_status,
               db_name, endpoint ->> 'address' as endpoint
        from aws_redshift_cluster
        order by cluster_identifier;
    """)

def get_rds_snapshots():
    return _safe_fetch("""
        select db_snapshot_identifier, db_instance_identifier, status, engine,
               create_time, allocated_storage, region
        from aws_rds_db_snapshot
        order by create_time desc;
    """)

def get_elasticache_clusters():
    return _safe_fetch("""
        select cache_cluster_id, engine, engine_version, cache_node_type,
               num_cache_nodes, cache_cluster_status, region
        from aws_elasticache_cluster
        order by cache_cluster_id;
    """)

def get_glacier_vaults():
    return _safe_fetch("""
        select vault_name, creation_date, vault_arn,
               number_of_archives, size_in_bytes
        from aws_glacier_vault
        order by vault_name;
    """)

def get_backup_plans():
    return _safe_fetch("""
        select name, backup_plan_id, creation_date, region
        from aws_backup_plan
        order by creation_date desc;
    """)

# ---------- boto3 API 호출 (예: SageMaker) ----------
# - 표준 재시도(Adaptive) + 커스텀 재시도
# - 페이지네이션 안전 처리
BOTO_RETRY_CFG = BotoConfig(
    retries={"max_attempts": int(os.getenv("BOTO_MAX_ATTEMPTS", "10")), "mode": "adaptive"},
    read_timeout=int(os.getenv("BOTO_READ_TIMEOUT", "60")),
    connect_timeout=int(os.getenv("BOTO_CONNECT_TIMEOUT", "10")),
)

def _boto_client(service: str, region: Optional[str] = None):
    region = region or os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2")
    return boto3.client(service, region_name=region, config=BOTO_RETRY_CFG)

def _paginate(client, op_name: str, result_key: str, **kwargs) -> Iterable[Dict[str, Any]]:
    """
    boto3 페이지네이터 헬퍼.
    """
    paginator = client.get_paginator(op_name)
    for page in paginator.paginate(**kwargs):
        for item in page.get(result_key, []):
            yield item

def get_sagemaker_feature_group() -> Dict[str, Dict[str, Any]]:
    """
    dict 반환. 실패 시 빈 dict.
    """
    try:
        client = _boto_client("sagemaker")
        groups: Dict[str, Dict[str, Any]] = {}
        # 페이지네이션 안전
        for fg in _paginate(client, "list_feature_groups", "FeatureGroupSummaries"):
            name = fg.get("FeatureGroupName")
            if not name:
                continue
            groups[name] = {
                "creation_time": fg.get("CreationTime", "").isoformat()
                if hasattr(fg.get("CreationTime"), "isoformat")
                else str(fg.get("CreationTime")),
                "status": fg.get("FeatureGroupStatus"),
            }
        return groups
    except (ClientError, BotoCoreError) as e:
        if _is_optin_or_access_error(e):
            logger.warning("SageMaker not available/unauthorized: %s", e)
            return {}
        # 과금/제한/일시 장애(Throttling 등) 메시지
        logger.error("boto3 error: %s", e)
        return {}
    except Exception as e:
        logger.exception("Unexpected boto3 error: %s", e)
        return {}

# ---------- Steampipe로 Glue, Kinesis, MSK 조회 ----------
def get_glue_catalog_database():
    return _safe_fetch("""
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
    return _safe_fetch("""
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
    return _safe_fetch("""
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
