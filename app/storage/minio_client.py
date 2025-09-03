from datetime import timedelta
from minio import Minio
from app.config_loader import configuration

client = Minio(
    configuration["minio"]["endpoint"],
    access_key=configuration["minio"]["access_key"],
    secret_key=configuration["minio"]["secret_key"],
    secure=configuration["minio"]["secure"],
)

def generate_presigned_upload_url(object_name: str, expiry_minutes: int = 10) -> str:
    bucket = configuration["minio"]["bucket"]

    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    return client.presigned_put_object(
        bucket,
        object_name,
        expires=timedelta(minutes=expiry_minutes),
    )

def download_file(object_path: str) -> bytes:
    """Download file from MinIO (object_path = bucket/object_name)."""
    bucket, object_name = object_path.split("/", 1)
    response = client.get_object(bucket, object_name)
    return response.read()

def create_bucket(bucket_name: str) -> dict:
    """Create a new bucket in MinIO if it doesn't exist."""
    try:
        # Check if bucket already exists
        if client.bucket_exists(bucket_name):
            return {
                "status": "exists",
                "message": f"Bucket '{bucket_name}' already exists",
                "bucket_name": bucket_name
            }
        
        # Create the bucket
        client.make_bucket(bucket_name)
        
        return {
            "status": "created",
            "message": f"Bucket '{bucket_name}' created successfully",
            "bucket_name": bucket_name
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to create bucket '{bucket_name}': {str(e)}",
            "bucket_name": bucket_name
        }

def list_buckets() -> list:
    """List all buckets in MinIO."""
    try:
        buckets = client.list_buckets()
        return [{"name": bucket.name, "creation_date": bucket.creation_date} for bucket in buckets]
    except Exception as e:
        return []
