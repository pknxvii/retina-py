from fastapi import Request
from app.api.controllers.base import BaseController
from app.api.services.document import DocumentService
from app.api.models.requests import GenerateUploadUrlRequest, IndexDocRequest
from app.api.models.responses import GenerateUploadUrlResponse, IndexDocResponse


class DocumentController(BaseController):
    """Controller for document-related operations."""
    
    def __init__(self):
        super().__init__()
        self.document_service = DocumentService()
    
    async def generate_upload_url(self, request: Request) -> GenerateUploadUrlResponse:
        """Generate a pre-signed upload URL for document upload."""
        headers = self.extract_headers(request)
        body = await request.json()
        request_data = GenerateUploadUrlRequest(**body)
        
        return self.document_service.generate_upload_url(request_data, headers)
    
    async def index_document(self, request: Request) -> IndexDocResponse:
        """Dispatch document indexing task."""
        headers = self.extract_headers(request)
        body = await request.json()
        request_data = IndexDocRequest(**body)
        
        return self.document_service.index_document(request_data, headers)
