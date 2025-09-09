from typing import Dict, Any
from app.utils.id_generator import generate_doc_id
from app.storage.minio_client import generate_presigned_upload_url
from app.dispatcher.celery_dispatcher import CeleryDispatcher
from app.api.models.requests import GenerateUploadUrlRequest, IndexDocRequest, HeaderData
from app.api.models.responses import GenerateUploadUrlResponse, IndexDocResponse


class DocumentService:
    """Service layer for document operations."""
    
    def __init__(self):
        self.dispatcher = CeleryDispatcher()
    
    def generate_upload_url(self, request_data: GenerateUploadUrlRequest, headers: HeaderData) -> GenerateUploadUrlResponse:
        """Generate a pre-signed upload URL for document upload."""
        doc_id = generate_doc_id()
        doc_type = request_data.doc_type
        object_name = f"{doc_id}.{doc_type}"
        object_path = f"{headers.organization_id}/{headers.user_id}/{object_name}"
        
        upload_url = generate_presigned_upload_url(object_name)
        
        return GenerateUploadUrlResponse(
            doc_id=doc_id,
            upload_url=upload_url,
            object_path=object_path
        )
    
    def index_document(self, request_data: IndexDocRequest, headers: HeaderData) -> IndexDocResponse:
        """Dispatch document indexing task."""
        task_data = {
            "doc_id": request_data.doc_id,
            "object_path": request_data.object_path,
            "user_id": headers.user_id,
            "organization_id": headers.organization_id
        }
        
        self.dispatcher.dispatch("index_document", task_data)
        
        return IndexDocResponse(
            doc_id=request_data.doc_id,
            status="Indexing dispatched"
        )
