import boto3
import zipfile
import io
import os
import importlib.util
from emd.revision import VERSION
from .logger_utils import get_logger
from .aws_service_utils import create_s3_bucket
logger = get_logger(__name__)

spec = importlib.util.find_spec("emd")
emd_package_dir = os.path.dirname(spec.origin)


def ziped_pipeline():
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED) as zipf:
        # Add pipeline folder files directly (requirements.txt, pipeline.py, etc.)
        pipeline_dir = os.path.join(emd_package_dir, 'pipeline')
        if os.path.exists(pipeline_dir):
            for root, dirs, files in os.walk(pipeline_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    # Create archive path relative to pipeline directory
                    arcname = os.path.relpath(file_path, pipeline_dir)
                    # Skip unwanted files
                    if ("artifacts" in arcname and "deploy" in arcname) \
                       or "__pycache__" in arcname:
                        continue
                    # Add to pipeline/ directory in zip
                    zip_arcname = os.path.join("pipeline", arcname)
                    zipf.write(file_path, zip_arcname)
        
        # Add EMD source code into pipeline/emd/
        for root, dirs, files in os.walk(emd_package_dir):
            for file in files:
                if file == "pipeline.zip":
                    continue
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, emd_package_dir)
                # Skip if it's already from pipeline directory (handled above)
                if arcname.startswith("pipeline"):
                    continue
                # Add EMD files to pipeline/emd/ directory
                emd_arcname = os.path.join("pipeline", "emd", arcname)
                zipf.write(file_path, emd_arcname)
        
        # Add cfn folder
        cfn_dir = os.path.join(emd_package_dir, "cfn")
        if os.path.exists(cfn_dir):
            for root, dirs, files in os.walk(cfn_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, emd_package_dir)
                    zipf.write(file_path, arcname)
    zip_buffer.seek(0)
    return zip_buffer


def upload_pipeline_source_to_s3(
        bucket,
        region,
        s3_key=f"{VERSION}/pipeline.zip"
    ):
    create_s3_bucket(
        bucket,
        region
    )
    zip_buffer = ziped_pipeline()
    s3 = boto3.client('s3', region_name=region)
    s3.upload_fileobj(zip_buffer, bucket, s3_key)
    return f"s3://{bucket}/{s3_key}"
