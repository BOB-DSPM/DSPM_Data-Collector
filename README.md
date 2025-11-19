# Docker 배포
```bash
# 1) 이미지 빌드
docker build -t comnyang/sage-collector:latest .

# 2) 컨테이너 실행 (필요한 환경변수는 상황에 맞게 수정)
docker run -d --name sage-collector \
  -p 8000:8000 \
  -e PORT=8000 \
  -e AWS_ACCESS_KEY_ID=YOUR_KEY \
  -e AWS_SECRET_ACCESS_KEY=YOUR_SECRET \
  -e AWS_DEFAULT_REGION=ap-northeast-2 \
  comnyang/sage-collector:latest

# 3) 로그 확인 (파일로 저장하고 싶다면 tee 사용)
docker logs -f sage-collector | tee log.txt

# 4) Docker Hub 업로드
docker login --username comnyang
docker push comnyang/sage-collector:latest
```

## 주요 환경 변수
| 이름 | 설명 | 기본값/비고 |
| --- | --- | --- |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | Steampipe와 boto3가 사용할 IAM 자격 증명 | 없음 |
| `AWS_REGION` / `AWS_DEFAULT_REGION` | boto3 기본 리전 | `ap-northeast-2` |
| `ALLOWED_REGIONS` | Steampipe 쿼리에서 허용할 리전 (쉼표 구분) | Opt-in 리전 자동 조회 |
| `PORT` | FastAPI/uvicorn 포트 | `8103` |
| `STEAMPIPE_DB_URL` | Steampipe PostgreSQL 전체 DSN | 없으면 아래 항목 사용 |
| `STEAMPIPE_DB_HOST` / `STEAMPIPE_DB_PORT` / `STEAMPIPE_DB_USER` / `STEAMPIPE_DB_PASSWORD` / `STEAMPIPE_DB_NAME` | Steampipe 서비스 위치 | `127.0.0.1:9193`, `steampipe`, 빈 패스워드 |
| `CORS_DEFAULT_ORIGINS` | 기본 허용 오리진 목록 | 로컬 dev 주소 4개 |
| `CORS_ALLOW_ORIGINS` | 추가 허용 오리진 목록 | 빈 문자열 |
| `CORS_ALLOW_ALL` | `true`면 `allow_origins=["*"]` (credentials 금지) | `false` |
| `SESSION_TTL_SEC`, `SESSION_CACHE_MAX`, `REDIS_URL` | API 응답 캐시 제어 | 600초, 512, 미사용 시 인메모리 |

---

# Steampipe 설치
### macOS 
```bash
brew install steampipe
```

### Ubuntu/Debian
```bash
curl -sL https://steampipe.io/install.sh | bash
```

### Windows (Powershell)
```powershell
irm https://steampipe.io/install.ps1 | iex
```
자세한 설치 문서는 https://steampipe.io/downloads

### AWS 플러그인 설치
```bash
steampipe plugin install aws
```

### Steampipe 실행
```bash
steampipe service start
```

### Python 환경 설정
```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

```bash
python -m uvicorn main:app --reload
```

## API 사용법
### GET api/s3-buckets
현재 계정의 모든 S3정보를 JSON 형식으로 반환합니다.

응답 예시
```json
[
  {
    "name": "my-mlops-dev-logs",
    "region": "ap-northeast-2",
    "creation_date": "2023-12-11T08:22:15"
  },
  ...
]
```

### GET api/ebs-volumes
현재 계정의 모든 S3 버킷 정보를 반환합니다.

응답예시
```json
[
  {
    "volume_id": "vol-0d30006b46f00b2a6",
    "size": 20,
    "availability_zone": "ap-northeast-2a",
    "encrypted": false,
    "name": null
  },
  {
    "volume_id": "vol-0e2a1db498ec621a3",
    "size": 20,
    "availability_zone": "ap-northeast-2b",
    "encrypted": false,
    "name": "data-volume-1"
  }
]

```


### GET api/efs-filesystems
EFS (Elastic File System) 리소스를 조회합니다.

응답예시
```json
[
  {
    "file_system_id": "fs-0a9ce49d363637f10",
    "creation_time": "2025-10-03T12:52:38+09:00",
    "size_in_bytes": {
      "Timestamp": "2025-10-04T06:23:34Z",
      "Value": 6144,
      "ValueInArchive": 0,
      "ValueInIA": 0,
      "ValueInStandard": 6144
    },
    "region": "ap-northeast-2"
  }
]

```

### GET api/fsx-filesystems
FSx (Windows, Lustre, NetApp 등) 파일 시스템을 조회합니다.

응답예시
```json
[
  {
    "file_system_id": "fsx-0123456789abcdef0",
    "storage_capacity": 1200,
    "type": "WINDOWS",
    "lifecycle": "AVAILABLE",
    "region": "us-west-2"
  }
]

```

### GET api/dynamodb-tables

DynamoDB 테이블 정보를 조회합니다.

응답 예시:

```json
[
  {
    "name": "Users",
    "table_status": "ACTIVE",
    "read_capacity": 5,
    "write_capacity": 5,
    "item_count": 124,
    "billing_mode": "PROVISIONED",
    "region": "ap-northeast-2"
  }
]
```

### GET api/rds-instances
Amazon RDS 인스턴스의 정보를 조회합니다. RDS는 MySQL, PostgreSQL 등 다양한 데이터베이스 엔진을 지원하는 AWS의 관리형 데이터베이스 서비스입니다.

응답예시
```json
[
  {
    "db_instance_identifier": "dspmeksstack-dspmdatabasea69d27a7-ykujqpxuyvw0",
    "engine": "postgres",
    "allocated_storage": 20,
    "status": "available",
    "endpoint_address": "dspmeksstack-dspmdatabasea69d27a7-ykujqpxuyvw0.cdsikyuewe0q.ap-northeast-2.rds.amazonaws.com",
    "class": "db.t3.micro"
  }
]

```

### GET api/redshift-clusters

Redshift 클러스터 정보를 조회합니다.

응답 예시:

```json
[
  {
    "cluster_identifier": "redshift-cluster-1",
    "node_type": "dc2.large",
    "number_of_nodes": 2,
    "cluster_status": "available",
    "db_name": "dev",
    "endpoint": "redshift-cluster-1.abc123xyz789.ap-northeast-2.redshift.amazonaws.com"
  }
]
```

## GET api/rds-snapshots

Amazon RDS 스냅샷 목록을 조회합니다. 스냅샷은 RDS 인스턴스의 시점 복원을 위한 백업 데이터입니다.

응답 예시:
```json
[
  {
    "db_snapshot_identifier": "rds:mydb-2025-10-04-06-30",
    "db_instance_identifier": "mydb",
    "status": "available",
    "engine": "postgres",
    "create_time": "2025-10-04T06:30:00Z",
    "allocated_storage": 20,
    "region": "ap-northeast-2"
  }
]
```

## GET api/elasticache-clusters

ElastiCache 클러스터 정보를 조회합니다. Redis 또는 Memcached 엔진 기반의 인메모리 데이터 스토어입니다.

응답 예시:
```json
[
  {
    "cache_cluster_id": "my-redis-cluster",
    "engine": "redis",
    "engine_version": "6.x",
    "cache_node_type": "cache.t3.micro",
    "num_cache_nodes": 1,
    "cache_cluster_status": "available",
    "region": "ap-northeast-2"
  }
]
```

## GET api/glacier-vaults

Amazon Glacier(Vault) 정보 조회. 장기 보관용 스토리지 솔루션입니다.

응답 예시:
```json
[
  {
    "vault_name": "my-archive-vault",
    "creation_date": "2024-01-15T12:00:00Z",
    "vault_arn": "arn:aws:glacier:ap-northeast-2:123456789012:vaults/my-archive-vault",
    "number_of_archives": 25,
    "size_in_bytes": 104857600
  }
]
```

## GET api/backup-plans

AWS Backup 서비스에서 정의된 백업 계획(Backup Plan)을 조회합니다.

응답 예시:
```json
[
  {
    "name": "daily-backup-plan",
    "backup_plan_id": "abcd1234-5678-efgh-9101-ijklmnopqrst",
    "creation_date": "2025-10-01T14:22:00Z",
    "region": "ap-northeast-2"
  }
]
```

## GET api/feature-groups

SageMaker Feature Store의 Feature Group 정보를 조회합니다.  
Feature Group은 ML 학습/예측 시 공통적으로 사용하는 피처 데이터를 관리하는 리소스입니다.

응답 예시:
```json
[
  {
    "feature_group_name": "customer-churn-features",
    "record_identifier_feature_name": "customer_id",
    "event_time_feature_name": "event_timestamp",
    "feature_group_status": "Created",
    "creation_time": "2025-09-30T10:12:44Z",
    "region": "ap-northeast-2"
  }
]
```

## GET api/glue-databases

AWS Glue 데이터 카탈로그(Database) 정보를 조회합니다.
Glue Catalog는 데이터 레이크/ETL 파이프라인에서 사용하는 메타데이터 저장소입니다.

응답 예시:
```json
[
  {
    "name": "mlops_metadata",
    "description": "MLOps pipeline metadata database",
    "create_time": "2025-09-29T04:11:00Z",
    "region": "ap-northeast-2"
  }
]

```

## GET api/glue-databases

Amazon Kinesis Data Streams 정보를 조회합니다.
실시간 데이터 스트리밍 파이프라인을 위한 리소스입니다.

응답 예시:
```json
[
  {
    "stream_name": "mlops-ingestion-stream",
    "stream_status": "ACTIVE",
    "stream_mode": "PROVISIONED",
    "shard_count": 2,
    "region": "ap-northeast-2"
  }
]


```

## GET api/msk-clusters

Amazon MSK (Managed Streaming for Apache Kafka) 클러스터 정보를 조회합니다.
Kafka 기반 실시간 데이터 스트리밍 파이프라인을 관리할 수 있습니다.

응답 예시:
```json
[
  {
    "stream_name": "mlops-ingestion-stream",
    "stream_status": "ACTIVE",
    "stream_mode": "PROVISIONED",
    "shard_count": 2,
    "region": "ap-northeast-2"
  }
]


```




## GET api/all-resources

AWS 계정의 모든 주요 스토리지/데이터베이스 리소스를 한 번에 JSON으로 반환합니다.
S3, EBS, EFS, FSx, RDS, DynamoDB, Redshift, ElastiCache, Glacier, Backup 등 포함됩니다.

응답 예시:
```json
{
  "s3_buckets": [
    {
      "name": "amazon-sagemaker-651706765732-ap-northeast-2-260d12a5d02d",
      "region": "ap-northeast-2",
      "creation_date": "2025-10-04T06:07:44+00:00"
    },
    {
      "name": "mlopsdemo-churnxgboostpipelinestack-asset-ap-northeast-2-65170",
      "region": "ap-northeast-2",
      "creation_date": "2025-10-04T09:32:36+00:00"
    }
  ],
  "ebs_volumes": [
    {
      "volume_id": "vol-0d30006b46f00b2a6",
      "size": 20,
      "availability_zone": "ap-northeast-2a",
      "encrypted": false,
      "name": null
    }
  ],
  "efs_filesystems": [
    {
      "file_system_id": "fs-0861e94265a837e07",
      "creation_time": "2025-10-03T03:39:51+00:00",
      "size_in_bytes": {
        "Value": 6144,
        "Timestamp": "2025-10-04T09:45:40Z",
        "ValueInIA": 0,
        "ValueInArchive": 0,
        "ValueInStandard": 6144
      },
      "region": "ap-northeast-2"
    }
  ],
  "fsx_filesystems": [],
  "rds_instances": [
    {
      "db_instance_identifier": "dspmeksstack-dspmdatabasea69d27a7-ykujqpxuyvw0",
      "engine": "postgres",
      "allocated_storage": 20,
      "status": "available",
      "endpoint_address": "dspmeksstack-dspmdatabasea69d27a7-ykujqpxuyvw0.cdsikyuewe0q.ap-northeast-2.rds.amazonaws.com",
      "class": "db.t3.micro"
    }
  ],
  "rds_snapshots": [
    {
      "db_snapshot_identifier": "rds:dspmeksstack-dspmdatabasea69d27a7-ykujqpxuyvw0-2025-10-03-16-02",
      "db_instance_identifier": "dspmeksstack-dspmdatabasea69d27a7-ykujqpxuyvw0",
      "status": "available",
      "engine": "postgres",
      "create_time": "2025-10-03T16:02:23.570000+00:00",
      "allocated_storage": 20,
      "region": "ap-northeast-2"
    }
  ],
  "dynamodb_tables": [],
  "redshift_clusters": [],
  "elasticache_clusters": [],
  "glacier_vaults": [],
  "backup_plans": [],
  "feature-groups": {
    "ad-click-feature-group-dev": {
      "creation_time": "2025-09-30T10:44:13.318000+09:00",
      "status": "Created"
    },
    "ad-click-feature-group": {
      "creation_time": "2025-09-19T17:46:13.427000+09:00",
      "status": "Created"
    },
    "ad-click-dev-feature-group": {
      "creation_time": "2025-10-01T10:56:23.868000+09:00",
      "status": "Created"
    }
  },
  "glue-databases": [
    {
      "name": "sagemaker_featurestore",
      "description": null,
      "location_uri": null,
      "create_time": "2025-10-03T03:41:02+00:00",
      "catalog_id": "651706765732",
      "region": "ap-northeast-2"
    }
  ],
  "kinesis-streams": [],
  "msk-clusters": []
}
```

```text
지원하는 상세 조회 함수들:
S3: get_s3_bucket_detail(bucket_name)
EFS: get_efs_filesystem_detail(file_system_id)
FSx: get_fsx_filesystem_detail(file_system_id)
RDS: get_rds_instance_detail(db_identifier)
DynamoDB: get_dynamodb_table_detail(table_name)
Redshift: get_redshift_cluster_detail(cluster_id)
RDS Snapshot: get_rds_snapshot_detail(snapshot_id)
ElastiCache: get_elasticache_cluster_detail(cluster_id)
Glacier: get_glacier_vault_detail(vault_name)
AWS Backup: get_backup_plan_detail(plan_id)
SageMaker Feature Group: get_sagemaker_feature_group_detail(feature_group_name)
Glue Catalog Database: get_glue_database_detail(name)
Kinesis Stream: get_kinesis_stream_detail(stream_name)
MSK Cluster: get_msk_cluster_detail(cluster_name)
GET /api/repositories/s3/{bucket_name}
GET /api/repositories/efs/{file_system_id}
GET /api/repositories/fsx/{file_system_id}
GET /api/repositories/rds/{db_identifier}
GET /api/repositories/dynamodb/{table_name}
GET /api/repositories/redshift/{cluster_id}
GET /api/repositories/rds-snapshot/{snapshot_id}
GET /api/repositories/elasticache/{cluster_id}
GET /api/repositories/glacier/{vault_name}
GET /api/repositories/backup/{plan_id}
GET /api/repositories/feature-group/{feature_group_name}
GET /api/repositories/glue/{name}
GET /api/repositories/kinesis/{stream_name}
GET /api/repositories/msk/{cluster_name}
```

## 주의사항
반드시 steampipe service start 로 로컬 Steampipe 서버가 실행 중이어야 합니다.

AWS 자격증명은 CLI에서 동작하는 형태와 동일하게 설정되어야 합니다.
