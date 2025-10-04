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
## API 사용법
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

## 주의사항
반드시 steampipe service start 로 로컬 Steampipe 서버가 실행 중이어야 합니다.

AWS 자격증명은 CLI에서 동작하는 형태와 동일하게 설정되어야 합니다.