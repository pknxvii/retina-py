from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional


class BaseResponse(BaseModel):
    status: str = Field(..., description="Response status")


class HealthResponse(BaseResponse):
    status: str = "healthy"


class GenerateUploadUrlResponse(BaseModel):
    doc_id: str = Field(..., description="Generated document ID")
    upload_url: str = Field(..., description="Pre-signed upload URL")
    object_path: str = Field(..., description="Object path in storage")


class IndexDocResponse(BaseResponse):
    doc_id: str = Field(..., description="Document ID")
    status: str = Field(default="Indexing dispatched", description="Operation status")


class QueryResponse(BaseModel):
    answer: str = Field(..., description="The answer to the query")
    sources: Optional[List[str]] = Field(default=None, description="Sources used to generate the answer")


class BucketInfo(BaseModel):
    name: str = Field(..., description="Bucket name")
    creation_date: Optional[str] = Field(None, description="Bucket creation date")


class ListBucketsResponse(BaseModel):
    buckets: List[BucketInfo] = Field(default_factory=list, description="List of available buckets")


class CreateBucketResponse(BaseModel):
    organization_id: str = Field(..., description="Organization ID")
    bucket_name: str = Field(..., description="Created bucket name")
    status: str = Field(..., description="Operation status")
    message: str = Field(..., description="Operation message")


class OrganizationStats(BaseModel):
    factory_instance_id: str = Field(..., description="Factory instance ID")
    multi_tenant_stats: Dict[str, Any] = Field(default_factory=dict, description="Multi-tenant statistics")
