# apps/mlops_storage.py
from __future__ import annotations
import os
from typing import Dict, List, Set, Tuple, Any, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = os.getenv("AWS_REGION", "ap-northeast-2")
_sm = boto3.client("sagemaker", region_name=REGION, config=Config(retries={"max_attempts": 10, "mode": "standard"}))
_s3 = boto3.client("s3", region_name=REGION)

def _add_s3_uri(uri: Optional[str], s3_buckets: Set[str], s3_prefixes: Set[str]):
    if not uri or not uri.startswith("s3://"):
        return
    path = uri[5:]
    parts = path.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    s3_buckets.add(bucket)
    if prefix:
        s3_prefixes.add(f"{bucket}/{prefix}")

def _collect_from_processing(job: Dict[str, Any], s3_buckets: Set[str], s3_prefixes: Set[str], efs_ids: Set[str], fsx_ids: Set[str], redshift_refs: Set[str], glue_refs: Set[str]):
    # Inputs
    for pin in job.get("ProcessingInputs", []) or []:
        ds = pin.get("DatasetDefinition")
        if ds:
            if "RedshiftDatasetDefinition" in ds:
                r = ds["RedshiftDatasetDefinition"]
                cluster_id = r.get("ClusterIdentifier")
                db = r.get("Database")
                table = r.get("TableName")
                redshift_refs.add(f"{cluster_id or 'redshift'}::{db or ''}.{table or ''}")
            if "AthenaDatasetDefinition" in ds:
                a = ds["AthenaDatasetDefinition"]
                database = a.get("Catalog") or a.get("Database")
                table = a.get("TableName")
                glue_refs.add(f"{database or 'glue'}::{table or ''}")
            # S3Output for DatasetDefinition
            _add_s3_uri(ds.get("DataDistributionType"), s3_buckets, s3_prefixes)  # rarely uri, keep safe
        s3_in = pin.get("S3Input")
        if s3_in:
            _add_s3_uri(s3_in.get("S3Uri"), s3_buckets, s3_prefixes)
    # Outputs
    out_cfg = job.get("ProcessingOutputConfig") or {}
    for pout in out_cfg.get("Outputs", []) or []:
        s3_out = pout.get("S3Output")
        if s3_out:
            _add_s3_uri(s3_out.get("S3Uri"), s3_buckets, s3_prefixes)
    # FileSystemConfig (rare on processing, but support)
    fs_cfg = job.get("ProcessingResources", {}).get("ClusterConfig", {}).get("InstanceStorageConfig", {})
    # No direct FSx/EFS id here typically; skip

def _collect_from_training(job: Dict[str, Any], s3_buckets: Set[str], s3_prefixes: Set[str], efs_ids: Set[str], fsx_ids: Set[str]):
    # Channels
    for ch in job.get("InputDataConfig", []) or []:
        ds = ch.get("DataSource") or {}
        s3s = ds.get("S3DataSource")
        if s3s:
            _add_s3_uri(s3s.get("S3Uri"), s3_buckets, s3_prefixes)
        fss = ds.get("FileSystemDataSource")
        if fss:
            fstype = fss.get("FileSystemType")
            fsid = fss.get("FileSystemId")
            if fstype == "EFS":
                efs_ids.add(fsid)
            elif fstype == "FSxLustre":
                fsx_ids.add(fsid)
    # Outputs
    out = job.get("OutputDataConfig") or {}
    _add_s3_uri(out.get("S3OutputPath"), s3_buckets, s3_prefixes)
    # Checkpoint / Debug / Profiler
    chk = job.get("CheckpointConfig") or {}
    _add_s3_uri(chk.get("S3Uri"), s3_buckets, s3_prefixes)
    dbg = job.get("DebugHookConfig") or {}
    _add_s3_uri(dbg.get("S3OutputPath"), s3_buckets, s3_prefixes)
    prof = job.get("ProfilerConfig") or {}
    _add_s3_uri(prof.get("ProfilingOutputPath"), s3_buckets, s3_prefixes)
    # Model Artifacts
    ma = job.get("ModelArtifacts") or {}
    _add_s3_uri(ma.get("S3ModelArtifacts"), s3_buckets, s3_prefixes)

def _collect_from_transform(job: Dict[str, Any], s3_buckets: Set[str], s3_prefixes: Set[str]):
    # Transform input
    ti = job.get("TransformInput", {}) or {}
    ds = ti.get("DataSource", {}) or {}
    s3data = ds.get("S3DataSource")
    if s3data:
        _add_s3_uri(s3data.get("S3Uri"), s3_buckets, s3_prefixes)
    # Output
    to = job.get("TransformOutput", {}) or {}
    _add_s3_uri(to.get("S3OutputPath"), s3_buckets, s3_prefixes)

def _collect_from_endpoint_data_capture(endpoint_name: str, s3_buckets: Set[str], s3_prefixes: Set[str]):
    try:
        ep = _sm.describe_endpoint(EndpointName=endpoint_name)
        epc_name = ep.get("EndpointConfigName")
        if not epc_name:
            return
        ec = _sm.describe_endpoint_config(EndpointConfigName=epc_name)
        dcc = ec.get("DataCaptureConfig") or {}
        _add_s3_uri(dcc.get("DestinationS3Uri"), s3_buckets, s3_prefixes)
    except ClientError:
        pass

def _collect_from_feature_store(s3_buckets: Set[str], s3_prefixes: Set[str], dynamodb_tables: Set[str]):
    # Offline: S3, Online: DynamoDB
    paginator = _sm.get_paginator("list_feature_groups")
    for page in paginator.paginate():
        for fg in page.get("FeatureGroupSummaries", []) or []:
            name = fg.get("FeatureGroupName")
            try:
                desc = _sm.describe_feature_group(FeatureGroupName=name)
            except ClientError:
                continue
            offline = (desc.get("OfflineStoreConfig") or {}).get("S3StorageConfig") or {}
            _add_s3_uri(offline.get("ResolvedOutputS3Uri") or offline.get("S3Uri"), s3_buckets, s3_prefixes)
            online = desc.get("OnlineStoreConfig") or {}
            if online.get("EnableOnlineStore"):
                os_cfg = online.get("OnlineStoreConfig") or {}
                tab = os_cfg.get("SecurityConfig", {}).get("KmsKeyId")  # not the table
            # DynamoDB table name
            ddb_table = (desc.get("OnlineStoreConfig") or {}).get("SecurityConfig", {}).get("KmsKeyId")
            # KmsKeyId is not table; real table name isn’t in API. Infer via "DescribeFeatureGroup" -> "OnlineStoreConfig" lacks table.
            # We leave DynamoDB table unresolved (SageMaker manages, name not exposed). Mark presence only.
            if online.get("EnableOnlineStore"):
                dynamodb_tables.add(f"(managed by SageMaker) OnlineStore for {name}")

def _list_all_pipelines() -> List[str]:
    names = []
    paginator = _sm.get_paginator("list_pipelines")
    for page in paginator.paginate():
        for p in page.get("PipelineSummaries", []) or []:
            if p.get("PipelineName"):
                names.append(p["PipelineName"])
    return names

def _iter_pipeline_steps(pipeline_name: str):
    # list recent executions; broaden if needed
    exec_paginator = _sm.get_paginator("list_pipeline_executions")
    for ep in exec_paginator.paginate(PipelineName=pipeline_name):
        for ex in ep.get("PipelineExecutionSummaries", []) or []:
            arn = ex.get("PipelineExecutionArn")
            if not arn:
                continue
            step_paginator = _sm.get_paginator("list_pipeline_execution_steps")
            for sp in step_paginator.paginate(PipelineExecutionArn=arn):
                for step in sp.get("PipelineExecutionSteps", []) or []:
                    yield step

def _safe_describe_training(name: str) -> Dict[str, Any]:
    try:
        return _sm.describe_training_job(TrainingJobName=name)
    except ClientError:
        return {}

def _safe_describe_processing(name: str) -> Dict[str, Any]:
    try:
        return _sm.describe_processing_job(ProcessingJobName=name)
    except ClientError:
        return {}

def _safe_describe_transform(name: str) -> Dict[str, Any]:
    try:
        return _sm.describe_transform_job(TransformJobName=name)
    except ClientError:
        return {}

def _collect_from_model_artifacts(model_name: str, s3_buckets: Set[str], s3_prefixes: Set[str]):
    try:
        m = _sm.describe_model(ModelName=model_name)
    except ClientError:
        return
    pri = m.get("PrimaryContainer") or {}
    _add_s3_uri(pri.get("ModelDataUrl"), s3_buckets, s3_prefixes)
    # Multi-container
    for c in m.get("Containers", []) or []:
        _add_s3_uri(c.get("ModelDataUrl"), s3_buckets, s3_prefixes)

def get_sagemaker_mlops_storage_map() -> Dict[str, Any]:
    """
    SageMaker Pipeline과 그 하위 리소스에서 참조되는 모든 저장소 식별자(S3, EFS, FSx, FeatureStore, Endpoint DataCapture 등)를 수집
    """
    s3_buckets: Set[str] = set()
    s3_prefixes: Set[str] = set()
    efs_ids: Set[str] = set()
    fsx_ids: Set[str] = set()
    redshift_refs: Set[str] = set()
    glue_refs: Set[str] = set()
    endpoints_seen: Set[str] = set()
    models_seen: Set[str] = set()

    # 1) Feature Store (전역)
    _collect_from_feature_store(s3_buckets, s3_prefixes, dynamodb_tables := set())

    # 2) 파이프라인 → 실행 → 스텝 순회
    for pipe in _list_all_pipelines():
        for step in _iter_pipeline_steps(pipe):
            meta = step.get("Metadata") or {}
            stype = step.get("StepType")
            # Training
            if "TrainingJob" in meta or stype == "Training":
                tj_name = (meta.get("TrainingJob") or {}).get("Arn", "").split("/")[-1] or meta.get("TrainingJob", {}).get("Name")
                if tj_name:
                    tj = _safe_describe_training(tj_name)
                    if tj:
                        _collect_from_training(tj, s3_buckets, s3_prefixes, efs_ids, fsx_ids)
                        # 모델 아티팩트
                        ma = tj.get("ModelArtifacts", {})
                        _add_s3_uri(ma.get("S3ModelArtifacts"), s3_buckets, s3_prefixes)
            # Processing
            if "ProcessingJob" in meta or stype == "Processing":
                pj_name = (meta.get("ProcessingJob") or {}).get("Arn", "").split("/")[-1] or meta.get("ProcessingJob", {}).get("Name")
                if pj_name:
                    pj = _safe_describe_processing(pj_name)
                    if pj:
                        _collect_from_processing(pj, s3_buckets, s3_prefixes, efs_ids, fsx_ids, redshift_refs, glue_refs)
            # Transform
            if "TransformJob" in meta or stype == "Transform":
                xj_name = (meta.get("TransformJob") or {}).get("Arn", "").split("/")[-1] or meta.get("TransformJob", {}).get("Name")
                if xj_name:
                    xj = _safe_describe_transform(xj_name)
                    if xj:
                        _collect_from_transform(xj, s3_buckets, s3_prefixes)
            # RegisterModel → ModelName
            if "RegisterModel" in meta or stype == "RegisterModel":
                reg = meta.get("RegisterModel") or {}
                model_pkg_arn = reg.get("ModelPackageArn")
                model_name = reg.get("ModelPackageGroupName") or reg.get("ModelName")
                if model_name:
                    models_seen.add(model_name)

            # Model step might appear explicitly
            if "Model" in meta:
                model_name = (meta.get("Model") or {}).get("ModelName")
                if model_name:
                    models_seen.add(model_name)

            # CreateEndpoint / UpdateEndpoint
            if "Endpoint" in meta or stype in ("CreateModel", "CreateEndpoint", "UpdateEndpoint"):
                ep_name = (meta.get("Endpoint") or {}).get("EndpointName")
                if ep_name:
                    endpoints_seen.add(ep_name)

    # 3) 모델 아티팩트 (스텝에서 발견된 모델명들)
    for mn in list(models_seen):
        _collect_from_model_artifacts(mn, s3_buckets, s3_prefixes)

    # 4) 엔드포인트 DataCapture S3
    for en in list(endpoints_seen):
        _collect_from_endpoint_data_capture(en, s3_buckets, s3_prefixes)

    # 5) 엔티티 정렬/출력
    return {
        "region": REGION,
        "s3": {
            "buckets": sorted(s3_buckets),
            "bucket_prefixes": sorted(s3_prefixes),  # "bucket/prefix" 형태
        },
        "file_systems": {
            "efs_ids": sorted(filter(None, efs_ids)),
            "fsx_ids": sorted(filter(None, fsx_ids)),
        },
        "feature_store": {
            "online_store_tables": sorted(dynamodb_tables),  # 실제 테이블명은 비공개 관리됨 → 존재 표시
        },
        "external_sources": {
            "redshift": sorted(redshift_refs),
            "glue_athena": sorted(glue_refs),
        },
        "discovered_models": sorted(models_seen),
        "discovered_endpoints": sorted(endpoints_seen),
    }
