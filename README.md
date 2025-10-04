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
### GET /s3-buckets
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

### GET /ebs-volumes
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


### GET /efs-filesystems
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

### GET /fsx-filesystems
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

### GET /dynamodb-tables

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

### GET /rds-instances
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

### GET /redshift-clusters

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

## GET /rds-snapshots

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

## GET /elasticache-clusters

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

## GET /glacier-vaults

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

## GET /backup-plans

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


## 주의사항
반드시 steampipe service start 로 로컬 Steampipe 서버가 실행 중이어야 합니다.

AWS 자격증명은 CLI에서 동작하는 형태와 동일하게 설정되어야 합니다.