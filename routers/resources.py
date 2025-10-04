from fastapi import APIRouter
import collector


router = APIRouter()

@router.get("/s3-buckets")
def s3_buckets():
    return collector.get_s3_buckets()

@router.get("/ebs-volumes")
def ebs_volumes():
    return collector.get_ebs_volumes()

@router.get("/efs-filesystems")
def efs_filesystems():
    return collector.get_efs_filesystems()

@router.get("/fsx-filesystems")
def fsx_filesystems():
    return collector.get_fsx_filesystems()

@router.get("/rds-instances")
def rds_instances():
    return collector.get_rds_instances()

@router.get("/dynamodb-tables")
def dynamodb_tables():
    return collector.get_dynamodb_tables()

@router.get("/redshift-clusters")
def redshift_clusters():
    return collector.get_redshift_clusters()

@router.get("/rds-snapshots")
def rds_snapshots():
    return collector.get_rds_snapshots()

@router.get("/elasticache-clusters")
def elasticache_clusters():
    return collector.get_elasticache_clusters()

@router.get("/glacier-vaults")
def glacier_vaults():
    return collector.get_glacier_vaults()

@router.get("/backup-plans")
def backup_plans():
    return collector.get_backup_plans()

@router.get("/feature-groups")
def sagemaker_feature_groups():
    return collector.get_sagemaker_feature_group()

@router.get("/glue-databases")
def backup_plans():
    return collector.get_glue_catalog_database()

@router.get("/kinesis-streams")
def backup_plans():
    return collector.get_kinesis_stream()

@router.get("/msk-clusters")
def backup_plans():
    return collector.get_msk_cluster()

# 전체 리소스를 한 번에 반환하는 API
@router.get("/all-resources")
def all_resources():
    return {
        "s3_buckets": collector.get_s3_buckets(),
        "ebs_volumes": collector.get_ebs_volumes(),
        "efs_filesystems": collector.get_efs_filesystems(),
        "fsx_filesystems": collector.get_fsx_filesystems(),
        "rds_instances": collector.get_rds_instances(),
        "rds_snapshots": collector.get_rds_snapshots(),
        "dynamodb_tables": collector.get_dynamodb_tables(),
        "redshift_clusters": collector.get_redshift_clusters(),
        "elasticache_clusters": collector.get_elasticache_clusters(),
        "glacier_vaults": collector.get_glacier_vaults(),
        "backup_plans": collector.get_backup_plans(),
        "feature-groups": collector.get_sagemaker_feature_group(),
        "glue-databases": collector.get_glue_catalog_database(),
        "kinesis-streams": collector.get_kinesis_stream(),
        "msk-clusters": collector.get_msk_cluster(),
        
    }
