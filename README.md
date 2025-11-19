# DSPM Data Collector

AWS 리소스(S3, RDS, EFS 등)의 메타데이터를 Steampipe를 통해 수집하는 FastAPI 서비스입니다.

## 주요 기능

- **통합 조회**: 모든 AWS 스토리지/데이터베이스 리소스를 한 번에 조회
- **개별 조회**: 리소스 타입별 목록 및 상세 정보 조회
- **캐싱**: Redis 기반 세션 캐싱으로 성능 최적화
- **ETag 지원**: 효율적인 캐시 검증
- **Docker 지원**: 컨테이너 기반 간편한 배포

## 지원 리소스

### 스토리지
- S3 (Simple Storage Service)
- EBS (Elastic Block Store)
- EFS (Elastic File System)
- FSx (Windows, Lustre, NetApp 등)
- Glacier (장기 보관)

### 데이터베이스
- RDS (MySQL, PostgreSQL 등)
- DynamoDB
- Redshift
- ElastiCache (Redis, Memcached)

### 데이터 파이프라인 & ML
- SageMaker Feature Store
- AWS Glue Catalog
- Kinesis Data Streams
- MSK (Managed Kafka)

### 백업
- RDS Snapshots
- AWS Backup Plans

## 프로젝트 구조
```
.
├── main.py                 # FastAPI 진입점
├── apps/
│   ├── collector.py       # 리소스 수집 로직
│   ├── explorer.py        # 리소스 탐색
│   └── inspector.py       # 상세 정보 조회
├── routers/
│   ├── resources.py       # 리소스 목록 API
│   ├── repository.py      # 통합 조회 API
│   └── explorer_router.py # 탐색 API
├── utils/
│   ├── caching.py         # Redis 캐싱
│   ├── session_cache.py   # 세션 관리
│   └── etag_utils.py      # ETag 처리
├── docker/
│   ├── entrypoint.sh      # Docker 진입점
│   └── aws-init.sh        # AWS 초기화
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## 빠른 시작 (로컬)

### 1. Steampipe 설치

**macOS:**
```bash
brew install turbot/tap/steampipe
```

**Linux:**
```bash
sudo /bin/sh -c "$(curl -fsSL https://steampipe.io/install/steampipe.sh)"
```

**Windows (WSL2 필요):**
```bash
# WSL2 환경에서 Linux 명령어 사용
sudo /bin/sh -c "$(curl -fsSL https://steampipe.io/install/steampipe.sh)"
```

상세 설치 가이드: https://steampipe.io/downloads

### 2. AWS 플러그인 설치 및 실행
```bash
# AWS 플러그인 설치
steampipe plugin install aws

# Steampipe 서비스 시작
steampipe service start
```

### 3. 애플리케이션 설치 및 실행
```bash
# 가상환경 설정
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 의존성 설치
pip install -r requirements.txt

# 서버 실행 (포트 8103)
python -m uvicorn main:app --host 0.0.0.0 --port 8103 --reload
```

API 문서: http://localhost:8103/docs

## Docker로 실행

### Docker Compose 사용 (권장)
```bash
# 빌드 및 실행
docker-compose up -d

# 로그 확인
docker-compose logs -f

# 중지
docker-compose down
```

### Docker 직접 사용
```bash
# 이미지 빌드
docker build -t dspm-data-collector .

# 컨테이너 실행
docker run -d \
  -p 8103:8103 \
  -v ~/.aws:/home/appuser/.aws:ro \
  -e AWS_DEFAULT_REGION=ap-northeast-2 \
  --name dspm-collector \
  dspm-data-collector
```

> 프로덕션 환경(EKS/ECS/Fargate 등)에서는 컨테이너에 IAM Role을 연결해 자격 증명을 안전하게 위임하세요.

## 환경 변수

| 변수 | 설명 | 기본값 |
|------|------|--------|
| `PORT` | FastAPI 서버 포트 | 8103 |
| `AWS_DEFAULT_REGION` / `AWS_REGION` | boto3 및 Steampipe 기본 리전 | ap-northeast-2 |
| `ALLOWED_REGIONS` | Steampipe 쿼리 허용 리전(쉼표 구분) | Opt-in 리전 자동 감지 |
| `STEAMPIPE_DB_HOST` / `STEAMPIPE_DB_PORT` / `STEAMPIPE_DB_USER` / `STEAMPIPE_DB_NAME` | Steampipe PostgreSQL 연결 정보 | 127.0.0.1 / 9193 / steampipe / steampipe |
| `CORS_DEFAULT_ORIGINS` | 기본 허용 오리진 목록 | 로컬 개발 주소 4개 |
| `CORS_ALLOW_ORIGINS` | 추가 허용 오리진(쉼표 구분) | 빈 문자열 |
| `CORS_ALLOW_ALL` | `true` 시 모든 오리진 허용(`credentials=False` 필요) | `false` |
| `SESSION_TTL_SEC`, `SESSION_CACHE_MAX`, `REDIS_URL` | 응답 캐시 제어 | 600 / 512 / 인메모리 |

**CORS 설정 예시:**
```bash
export CORS_ALLOW_ORIGINS="https://admin.example.com,https://app.example.com"
```

## API 엔드포인트

### 전체 리소스 통합 조회
```bash
GET /api/repositories

# 모든 스토리지/데이터베이스 리소스를 한 번에 조회
curl -s http://localhost:8103/api/repositories | jq
```

**응답 구조:**
```json
{
  "s3_buckets": [...],
  "ebs_volumes": [...],
  "efs_filesystems": [...],
  "rds_instances": [...],
  "dynamodb_tables": [...],
  "redshift_clusters": [...],
  "elasticache_clusters": [...],
  "glacier_vaults": [...],
  "backup_plans": [...],
  "feature-groups": {...},
  "glue-databases": [...],
  "kinesis-streams": [...],
  "msk-clusters": [...]
}
```

### 리소스별 목록 조회

| 엔드포인트 | 설명 |
|-----------|------|
| `GET /api/s3-buckets` | S3 버킷 목록 |
| `GET /api/ebs-volumes` | EBS 볼륨 목록 |
| `GET /api/efs-filesystems` | EFS 파일시스템 목록 |
| `GET /api/fsx-filesystems` | FSx 파일시스템 목록 |
| `GET /api/rds-instances` | RDS 인스턴스 목록 |
| `GET /api/rds-snapshots` | RDS 스냅샷 목록 |
| `GET /api/dynamodb-tables` | DynamoDB 테이블 목록 |
| `GET /api/redshift-clusters` | Redshift 클러스터 목록 |
| `GET /api/elasticache-clusters` | ElastiCache 클러스터 목록 |
| `GET /api/glacier-vaults` | Glacier Vault 목록 |
| `GET /api/backup-plans` | AWS Backup 계획 목록 |
| `GET /api/feature-groups` | SageMaker Feature Group 목록 |
| `GET /api/glue-databases` | Glue Catalog 데이터베이스 목록 |
| `GET /api/kinesis-streams` | Kinesis Stream 목록 |
| `GET /api/msk-clusters` | MSK 클러스터 목록 |

### 리소스 상세 조회
```bash
# S3 버킷 상세
GET /api/repositories/s3/{bucket_name}

# RDS 인스턴스 상세
GET /api/repositories/rds/{db_identifier}

# DynamoDB 테이블 상세
GET /api/repositories/dynamodb/{table_name}
```

**지원하는 상세 조회:**
- `s3/{bucket_name}`
- `efs/{file_system_id}`
- `fsx/{file_system_id}`
- `rds/{db_identifier}`
- `dynamodb/{table_name}`
- `redshift/{cluster_id}`
- `rds-snapshot/{snapshot_id}`
- `elasticache/{cluster_id}`
- `glacier/{vault_name}`
- `backup/{plan_id}`
- `feature-group/{feature_group_name}`
- `glue/{name}`
- `kinesis/{stream_name}`
- `msk/{cluster_name}`

## 응답 예시

### S3 버킷 목록
```json
[
  {
    "name": "my-mlops-dev-logs",
    "region": "ap-northeast-2",
    "creation_date": "2023-12-11T08:22:15"
  }
]
```

### EBS 볼륨 목록
```json
[
  {
    "volume_id": "vol-0d30006b46f00b2a6",
    "size": 20,
    "availability_zone": "ap-northeast-2a",
    "encrypted": false,
    "name": null
  }
]
```

### RDS 인스턴스 목록
```json
[
  {
    "db_instance_identifier": "mydb-instance",
    "engine": "postgres",
    "allocated_storage": 20,
    "status": "available",
    "endpoint_address": "mydb.cdsikyuewe0q.ap-northeast-2.rds.amazonaws.com",
    "class": "db.t3.micro"
  }
]
```

## 필수 요구사항

### AWS 자격 증명

1. **IAM Role 사용 (권장)**  
   - Amazon EKS: 서비스 계정에 IAM Role(IRSA)을 연결합니다.  
   - Amazon ECS/Fargate: 태스크 실행 역할에 필요한 `ReadOnly` 정책을 부여합니다.  
   컨테이너는 메타데이터 서비스를 통해 임시 자격 증명을 자동으로 획득하므로 추가 설정이 필요 없습니다.

2. **로컬/테스트 환경 (AWS CLI 프로필 + 볼륨 마운트)**  
   ```bash
   aws configure --profile dspm
   export AWS_PROFILE=dspm
   docker run -d \
     -p 8103:8103 \
     -e AWS_PROFILE=$AWS_PROFILE \
     -v ~/.aws:/home/appuser/.aws:ro \
     dspm-data-collector
   ```
   `~/.aws` 디렉터리를 읽기 전용으로 마운트하여 컨테이너가 로컬 프로필을 사용할 수 있게 합니다.

### Steampipe 서비스

반드시 Steampipe 서비스가 실행 중이어야 합니다:
```bash
# 서비스 시작
steampipe service start

# 상태 확인
steampipe service status

# 재시작
steampipe service restart
```

### 필요한 IAM 권한
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListAllMyBuckets",
        "s3:GetBucketLocation",
        "ec2:DescribeVolumes",
        "elasticfilesystem:DescribeFileSystems",
        "fsx:DescribeFileSystems",
        "rds:DescribeDBInstances",
        "rds:DescribeDBSnapshots",
        "dynamodb:ListTables",
        "dynamodb:DescribeTable",
        "redshift:DescribeClusters",
        "elasticache:DescribeCacheClusters",
        "glacier:ListVaults",
        "backup:ListBackupPlans",
        "sagemaker:ListFeatureGroups",
        "glue:GetDatabases",
        "kinesis:ListStreams",
        "kafka:ListClusters"
      ],
      "Resource": "*"
    }
  ]
}
```

## 캐싱 및 성능

- **Redis 캐싱**: 반복 조회 성능 최적화
- **ETag 지원**: HTTP 캐시 검증으로 네트워크 트래픽 감소
- **세션 관리**: 요청별 캐시 세션 관리

## 트러블슈팅

### Steampipe 연결 실패
```bash
# 서비스 상태 확인
steampipe service status

# 재시작
steampipe service restart

# 플러그인 확인
steampipe plugin list
```

### AWS 권한 오류

응답에서 특정 리소스가 비어있다면 IAM 권한을 확인하세요.
```bash
# AWS CLI로 권한 테스트
aws s3 ls
aws rds describe-db-instances
aws dynamodb list-tables
```

### 포트 충돌
```bash
# 포트 변경
uvicorn main:app --port 8104 --reload

# 또는 환경변수
export PORT=8104
python -m uvicorn main:app --host 0.0.0.0 --port $PORT
```

### Docker 컨테이너에서 AWS 자격 증명 오류
```bash
# 볼륨 마운트 확인
docker run -v ~/.aws:/home/appuser/.aws:ro ...
# 또는 EKS/ECS 등의 실행 환경에 연결된 IAM Role 권한 확인
```

## 아키텍처
```
[AWS 계정] 
    ↓
[Steampipe Service] (localhost:9193, PostgreSQL 호환)
    ↓
[Redis Cache] (선택)
    ↓
[Data Collector API] (localhost:8103, FastAPI)
    ├─ /api/repositories (통합 조회)
    ├─ /api/s3-buckets (개별 조회)
    └─ /api/repositories/s3/{name} (상세 조회)
    ↓
[Client Applications]
```

## 의존성

- **fastapi**: REST API 프레임워크
- **uvicorn**: ASGI 서버
- **sqlalchemy**: Steampipe(PostgreSQL) 연결
- **psycopg2-binary**: PostgreSQL 드라이버
- **boto3**: AWS SDK (폴백용)
- **pandas**: 데이터 처리
- **redis>=5.0.0**: 캐싱
