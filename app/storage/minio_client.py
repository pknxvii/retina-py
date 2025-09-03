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
