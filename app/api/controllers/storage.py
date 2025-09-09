from fastapi import Request
from app.api.controllers.base import BaseController
from app.api.services.storage import StorageService
from app.api.models.responses import CreateBucketResponse, ListBucketsResponse


class StorageController(BaseController):
    """Controller for storage-related operations."""
    
    def __init__(self):
        super().__init__()
        self.storage_service = StorageService()
    
    async def create_organization_bucket(self, request: Request) -> CreateBucketResponse:
        """Create a MinIO bucket for an organization."""
        headers = self.extract_headers(request)
        organization_id = self.require_organization_id(headers.organization_id)
        
        return self.storage_service.create_organization_bucket(organization_id)
    
    def list_buckets(self) -> ListBucketsResponse:
        """List all available buckets in MinIO."""
        return self.storage_service.list_all_buckets()
