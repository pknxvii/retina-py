from fastapi import Request
from app.api.controllers.base import BaseController
from app.api.services.organization import OrganizationService
from app.api.models.responses import OrganizationStats, CreateCollectionResponse


class OrganizationController(BaseController):
    """Controller for organization-related operations."""
    
    def __init__(self):
        super().__init__()
        self.organization_service = OrganizationService()
    
    def get_organization_stats(self) -> OrganizationStats:
        """Get statistics about active organizations in the multi-tenant pipeline."""
        return self.organization_service.get_organization_stats()
    
    def create_collection(self, request: Request) -> CreateCollectionResponse:
        """Create a Qdrant collection for an organization."""
        headers = self.extract_headers(request)
        return self.organization_service.create_collection(headers.organization_id)
