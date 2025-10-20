from __future__ import annotations
import os, json, datetime as dt, math
from typing import Any, Dict, List, Set, Tuple, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from sqlalchemy import create_engine
import pandas as pd

REGION = os.getenv("AWS_REGION", "ap-northeast-2")
BCONF  = Config(retries={"max_attempts": 10, "mode": "standard"})

# Steampipe 조회용 (collector와 동일 DS)
_engine = create_engine("postgresql://steampipe@localhost:9193/steampipe")
def _sp(query: str) -> List[Dict[str, Any]]:
    df = pd.read_sql(query, _engine)
    return df.to_dict(orient="records")

# ── 가중치/패널티 (CloudTrail ↓) ────────────────────────────────────────────
W_ML_PRINCIPAL_POLICY = 40   # ML 실행역할/파이프라인 역할 정책에 버킷이 명시
W_CLOUDTRAIL_ACTIVITY = 20   # ↓ 40 → 20 (관측 누락 위험 보정)
W_PREFIX_MATCH        = 30   # SageMaker/FeatureStore 등 prefix 힌트 일치
W_POLICY_HARDENING    = 10   # VPCE 제한·서비스 프린시펄 조건 등 보안정책 가점
PENALTY_PUBLIC_POLICY = -30  # Public 정책
PENALTY_ACL_PUBLIC    = -30  # Public ACL

ML_PRINCIPAL_HINTS = (
    "SageMaker","sagemaker","SageMakerExecutionRole",
    "Glue","Athena","Redshift","Kinesis","EMR",
    "CodeBuild","CodePipeline","StepFunctions","Events",
)

# ── 공통 유틸 ────────────────────────────────────────────────────────────────
def _safe_json(s: str) -> Dict[str, Any]:
    try: return json.loads(s)
    except Exception: return {}

def _score_from_activity(total: int, ml_hits: int) -> int:
    # ML 이벤트에 가중치를 두고 로그스케일로 캡핑
    x = ml_hits * 2 + max(0, total - ml_hits)
    if x <= 0: return 0
    return max(0, min(W_CLOUDTRAIL_ACTIVITY,
                      int(round(W_CLOUDTRAIL_ACTIVITY * (math.log10(1 + x) / math.log10(11))))))

# ── MLOps prefix 힌트 수집 ───────────────────────────────────────────────────
def _collect_mlops_prefix_hints() -> Set[str]:
    try:
        from apps.mlops_storage import get_sagemaker_mlops_storage_map
        m = get_sagemaker_mlops_storage_map()
        return set(m.get("s3", {}).get("bucket_prefixes", []))  # "bucket/prefix"
    except Exception:
        return set()

# ── MLOps 시스템 라벨 휴리스틱 ──────────────────────────────────────────────
def _classify_mlops_systems_for_bucket(bucket: str, hints: Set[str]) -> List[str]:
    systems: set[str] = set()
    for hp in hints:
        if not hp.startswith(f"{bucket}/"):
            continue
        p = hp.lower()
        if "feature-store" in p or "feature_store" in p:
            systems.add("Feature Store (offline)")
        elif "datacapture" in p or "data-capture" in p:
            systems.add("Endpoint DataCapture")
        elif "/processing" in p or "processing-output" in p or "processing-input" in p:
            systems.add("SageMaker Processing")
        elif "/training" in p or "model-artifacts" in p or "checkpoints" in p:
            systems.add("SageMaker Training")
        elif "/transform" in p or "batch-transform" in p:
            systems.add("SageMaker Transform")
        elif "model" in p and ("artifacts" in p or p.endswith(".tar.gz")):
            systems.add("Model Artifacts")
        else:
            systems.add("Pipeline (Other)")
    return sorted(systems)

def _prefix_match(bucket: str, hints: Set[str]) -> Tuple[int, List[str]]:
    for hp in hints:
        if hp.startswith(f"{bucket}/"):
            return W_PREFIX_MATCH, [f"Prefix match: {hp}"]
    return 0, []

# ── IAM: ML 역할 정책에서 버킷 언급 여부 ────────────────────────────────────
def _policy_refers_bucket(doc: Dict[str, Any], bucket: str) -> bool:
    stmts = doc.get("Statement", [])
    if not isinstance(stmts, list): stmts = [stmts]
    def _flat(x): return x if isinstance(x, list) else [x]
    for st in stmts:
        for r in _flat(st.get("Resource")):
            if isinstance(r, str) and r.startswith("arn:aws:s3:::"):
                if r == f"arn:aws:s3:::{bucket}" or r.startswith(f"arn:aws:s3:::{bucket}/"):
                    return True
    return False

def _bucket_in_ml_policies(bucket: str) -> Tuple[bool, List[str]]:
    iam = boto3.client("iam", config=BCONF)
    hits: List[str] = []
    try:
        paginator = iam.get_paginator("list_roles")
        for page in paginator.paginate():
            for role in page.get("Roles", []):
                rn = role.get("RoleName", "")
                if not any(h in rn for h in ML_PRINCIPAL_HINTS):
                    continue
                # 인라인
                for pn in iam.list_role_policies(RoleName=rn).get("PolicyNames", []):
                    doc = iam.get_role_policy(RoleName=rn, PolicyName=pn)["PolicyDocument"]
                    if _policy_refers_bucket(doc, bucket): hits.append(rn); break
                # 관리형
                for ap in iam.list_attached_role_policies(RoleName=rn).get("AttachedPolicies", []):
                    v = iam.get_policy(PolicyArn=ap["PolicyArn"])["Policy"]["DefaultVersionId"]
                    ver = iam.get_policy_version(PolicyArn=ap["PolicyArn"], VersionId=v)["PolicyVersion"]["Document"]
                    if _policy_refers_bucket(ver, bucket): hits.append(rn); break
    except ClientError:
        pass
    hits = sorted(set(hits))
    return (len(hits) > 0, hits)

# ── CloudTrail: 최근 버킷 활동 집계 ─────────────────────────────────────────
def _cloudtrail_bucket_activity(bucket: str, lookback_days: int) -> Tuple[int, int]:
    ct = boto3.client("cloudtrail", region_name=REGION, config=BCONF)
    end = dt.datetime.utcnow()
    start = end - dt.timedelta(days=lookback_days)
    next_token = None
    total = 0; ml_hits = 0

    def _is_for_bucket(evt: Dict[str, Any]) -> bool:
        for r in evt.get("Resources", []) or []:
            if r.get("ResourceName") in (bucket, f"arn:aws:s3:::{bucket}"): return True
        raw = _safe_json(evt.get("CloudTrailEvent", "{}"))
        if (raw.get("requestParameters") or {}).get("bucketName") == bucket: return True
        for rr in (raw.get("resources") or []):
            if rr.get("ARN") == f"arn:aws:s3:::{bucket}" or rr.get("resourceName") == bucket: return True
        return False

    def _is_ml_principal(evt: Dict[str, Any]) -> bool:
        uid = evt.get("Username", "") or ""
        if any(h in uid for h in ML_PRINCIPAL_HINTS): return True
        raw = _safe_json(evt.get("CloudTrailEvent", "{}"))
        arn = (raw.get("userIdentity") or {}).get("arn", "") or ""
        return any(h in arn for h in ML_PRINCIPAL_HINTS)

    while True:
        params = {"StartTime": start, "EndTime": end, "MaxResults": 50}
        if next_token: params["NextToken"] = next_token
        try:
            out = ct.lookup_events(**params)
        except ClientError:
            break
        for evt in out.get("Events", []):
            if _is_for_bucket(evt):
                total += 1
                if _is_ml_principal(evt): ml_hits += 1
        next_token = out.get("NextToken")
        if not next_token: break

    return total, ml_hits

# ── S3 정책/공개 신호 ────────────────────────────────────────────────────────
def _policy_has_vpce_or_service_principal(doc: Dict[str, Any]) -> bool:
    stmts = doc.get("Statement", [])
    if not isinstance(stmts, list): stmts = [stmts]
    for s in stmts:
        cond = s.get("Condition", {})
        if "StringEquals" in cond and "aws:sourceVpce" in (cond["StringEquals"] or {}): return True
        pr = s.get("Principal")
        if isinstance(pr, dict):
            if any(k.lower() == "service" for k in pr.keys()): return True
    return False

def _bucket_policy_signals(bucket: str) -> Tuple[int, List[str], Dict[str, Any]]:
    s3 = boto3.client("s3", region_name=REGION, config=BCONF)
    score = 0; reasons: List[str] = []
    meta: Dict[str, Any] = {"public_access_block": None, "acl_public": False, "policy_hardening": False}

    try:
        pab = s3.get_public_access_block(Bucket=bucket)["PublicAccessBlockConfiguration"]
        meta["public_access_block"] = pab
        if not all(pab.get(k, False) for k in ("BlockPublicAcls","IgnorePublicAcls","BlockPublicPolicy","RestrictPublicBuckets")):
            score += PENALTY_PUBLIC_POLICY; reasons.append("PublicAccessBlock relaxed (penalty)")
    except ClientError:
        pass

    try:
        acl = s3.get_bucket_acl(Bucket=bucket)
        for g in acl.get("Grants", []):
            gr = g.get("Grantee", {})
            if gr.get("Type") == "Group" and "AllUsers" in (gr.get("URI") or ""):
                meta["acl_public"] = True
                score += PENALTY_ACL_PUBLIC; reasons.append("ACL grants AllUsers (penalty)")
                break
    except ClientError:
        pass

    try:
        pol = s3.get_bucket_policy(Bucket=bucket)
        doc = json.loads(pol["Policy"])
        if _policy_has_vpce_or_service_principal(doc):
            meta["policy_hardening"] = True
            score += W_POLICY_HARDENING; reasons.append("BucketPolicy VPCE/ServicePrincipal (+)")
    except ClientError:
        pass

    return score, reasons, meta

# ── 공개 API: S3 버킷 하나 점수화 + 메타 생성 ───────────────────────────────
def score_s3_bucket(bucket_row: Dict[str, Any], lookback_days: int, mlops_prefix_hints: Set[str]) -> Dict[str, Any]:
    bucket = bucket_row["name"]

    pol_score, pol_reasons, pol_meta = _bucket_policy_signals(bucket)
    ref_hit, roles = _bucket_in_ml_policies(bucket)
    ref_score = W_ML_PRINCIPAL_POLICY if ref_hit else 0
    total, ml_hits = _cloudtrail_bucket_activity(bucket, lookback_days)
    act_score = _score_from_activity(total, ml_hits)
    pre_score, pre_reasons = _prefix_match(bucket, mlops_prefix_hints)

    score = pol_score + ref_score + act_score + pre_score
    reasons = (pol_reasons
               + (["Referenced by ML principals: " + ", ".join(roles)] if ref_hit else [])
               + ([f"CloudTrail hits total={total}, ml={ml_hits} (+{act_score})"] if act_score else [])
               + pre_reasons)

    # MLOps 시스템 식별/미식별 분류
    systems = _classify_mlops_systems_for_bucket(bucket, mlops_prefix_hints)
    identified = len(systems) > 0
    exists_in = {
        "identified": identified,
        "systems": systems if identified else ["미식별"],
        "evidence_prefixes": [h for h in mlops_prefix_hints if h.startswith(f"{bucket}/")][:10],
    }

    return {
        **bucket_row,
        "_meta": {
            "score": score,
            "reasons": reasons,
            "signals": {
                "policy_score": pol_score,
                "policy_meta": pol_meta,
                "ml_principal_policy_hit": ref_hit,
                "ml_principal_roles": roles,
                "activity_total": total,
                "activity_ml_hits": ml_hits,
                "activity_score": act_score,
                "prefix_hint_score": pre_score,
            },
            "exists_in": exists_in,
            "classification": "identified" if identified else "unidentified"
        }
    }

# ── 다른 리소스는 자리만 만들어 0점(확장 지점) ──────────────────────────────
def neutral_annotate(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        **row,
        "_meta": {
            "score": 0,
            "reasons": [],
            "signals": {},
            "exists_in": {
                "identified": False,
                "systems": ["미식별"],
                "evidence_prefixes": []
            },
            "classification": "unidentified"
        }
    }

# ── 전체 Annotator ──────────────────────────────────────────────────────────
def annotate_all(data: Dict[str, Any], lookback_days: int = 14) -> Dict[str, Any]:
    hints = _collect_mlops_prefix_hints()

    # S3: 점수/메타 삽입
    data["s3_buckets"] = [
        score_s3_bucket(b, lookback_days, hints) for b in (data.get("s3_buckets") or [])
    ]

    # 나머지(초기 버전: 중립)
    for key in (
        "ebs_volumes","efs_filesystems","fsx_filesystems","rds_instances","rds_snapshots",
        "dynamodb_tables","redshift_clusters","elasticache_clusters","glacier_vaults",
        "backup_plans","feature_groups","glue_databases","kinesis_streams","msk_clusters"
    ):
        if key in data and isinstance(data[key], list):
            data[key] = [neutral_annotate(x) for x in data[key]]

    # mlops_storage 자체에도 요약 메타 부착
    if "mlops_storage" in data:
        data["mlops_storage"] = {
            **data["mlops_storage"],
            "_meta": {
                "note": "prefix hints source",
                "s3_prefix_hints_count": len(hints),
            }
        }
    return data
# apps/mlops_ranker.py (파일 하단 적당한 위치에 추가)

def annotate_s3_records(records: List[Dict[str, Any]], lookback_days: int = 14) -> List[Dict[str, Any]]:
    """
    inspector.get_s3_bucket_detail()의 결과 레코드 리스트에
    _meta(score/reasons/signals/exists_in/classification)을 주입한다.
    """
    hints = _collect_mlops_prefix_hints()
    return [score_s3_bucket(r, lookback_days, hints) for r in (records or [])]


# ── 랭킹 API용: 상위 후보 반환 ──────────────────────────────────────────────
def _list_s3_buckets() -> List[Dict[str, Any]]:
    return _sp("""
        select name, region, creation_date
        from aws_s3_bucket
        order by region, name;
    """)

def rank_s3_candidates(lookback_days: int = 14, top_k: int = 50, region: str = REGION) -> Dict[str, Any]:
    hints = _collect_mlops_prefix_hints()
    buckets = _list_s3_buckets()
    scored = [score_s3_bucket(b, lookback_days, hints) for b in buckets]
    scored.sort(key=lambda x: (-x["_meta"]["score"], x["name"]))
    return {
        "region": region,
        "lookback_days": lookback_days,
        "total_buckets": len(scored),
        "top_k": top_k,
        "items": scored[:top_k],
    }