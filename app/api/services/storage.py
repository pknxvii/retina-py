from typing import List
from fastapi import HTTPException
from app.storage.minio_client import create_bucket, list_buckets
from app.config_loader import configuration
from app.api.models.responses import CreateBucketResponse, ListBucketsResponse, BucketInfo


class StorageService:
    """Service layer for storage operations."""
    
    def __init__(self):
        self.tenancy_config = configuration["tenancy"]
    
    def create_organization_bucket(self, organization_id: str) -> CreateBucketResponse:
        """Create a MinIO bucket for an organization."""
        # Sanitize organization ID for bucket name
        org_prefix = self.tenancy_config["organization_prefix"]
        bucket_name = f"{org_prefix}-{organization_id.lower().replace('_', '-').replace(' ', '-')}"
        
        # Validate bucket name (MinIO naming requirements)
        if len(bucket_name) < 3 or len(bucket_name) > 63:
            raise HTTPException(
                status_code=400,
                detail="Organization ID creates invalid bucket name (too short or too long)"
            )
        
        result = create_bucket(bucket_name)
        
        if result["status"] == "error":
            raise HTTPException(
                status_code=500,
                detail=result["message"]
            )
        
        return CreateBucketResponse(
            organization_id=organization_id,
            bucket_name=result["bucket_name"],
            status=result["status"],
            message=result["message"]
        )
    
    def list_all_buckets(self) -> ListBucketsResponse:
        """List all available buckets in MinIO."""
        buckets_data = list_buckets()
        
        # Convert bucket data to BucketInfo objects
        bucket_infos = [
            BucketInfo(name=bucket.get("name", ""), creation_date=bucket.get("creation_date"))
            for bucket in buckets_data
        ]
        
        return ListBucketsResponse(buckets=bucket_infos)
