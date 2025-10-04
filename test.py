import boto3
import json

def get_sagemaker_model_package():
    client = boto3.client("sagemaker", region_name="ap-northeast-2")
    resp = client.list_model_packages()
    packages = {
        pkg["ModelPackageArn"]: {
            "model_package_group_name": pkg.get("ModelPackageGroupName"),
            "model_package_arn": pkg.get("ModelPackageArn"),
            "model_package_status": pkg.get("ModelPackageStatus"),
            "creation_time": pkg["CreationTime"].isoformat() if "CreationTime" in pkg else None,
        }
        for pkg in resp.get("ModelPackageSummaryList", [])
    }
    return packages

if __name__ == "__main__":
    print(json.dumps(get_sagemaker_model_package(), indent=2))
