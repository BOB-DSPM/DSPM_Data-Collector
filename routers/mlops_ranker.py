from fastapi import APIRouter, Query, Request, Response
import asyncio
from utils.etag_utils import etag_response
from apps import mlops_ranker

router = APIRouter()

async def _run_with_etag(request: Request, response: Response, fn, *args, **kwargs):
    data = await asyncio.to_thread(fn, *args, **kwargs)
    return etag_response(request, response, data)

@router.get("/mlops/s3-candidates", tags=["MLOps Candidates"])
async def mlops_s3_candidates(
    request: Request,
    response: Response,
    days: int = Query(14, ge=1, le=90, description="CloudTrail 조회 일수(최대 90)"),
    top_k: int = Query(50, ge=1, le=500, description="상위 K개 반환"),
    region: str = Query("ap-northeast-2", description="기본 조회 리전"),
):
    """
    여러 신호들을 결합해 'MLOps에서 실제 사용 중일 가능성'이 높은 S3 버킷을 점수화해 반환
    - 신호: ML Principal 연계, CloudTrail 데이터/매니지먼트 이벤트, 메타데이터/프리픽스 매칭,
            버킷 정책/공개 설정(패널티) 등
    """
    return await _run_with_etag(
        request, response,
        mlops_ranker.rank_s3_candidates,
        lookback_days=days,
        top_k=top_k,
        region=region
    )