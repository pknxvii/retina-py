from fastapi import APIRouter, Request
from app.api.controllers.health import HealthController
from app.api.controllers.document import DocumentController
from app.api.controllers.storage import StorageController
from app.api.controllers.organization import OrganizationController
from app.api.controllers.query import QueryController
from app.api.models.requests import QueryRequest
from app.api.models.responses import (
    HealthResponse, GenerateUploadUrlResponse, IndexDocResponse,
    CreateBucketResponse, ListBucketsResponse, OrganizationStats, QueryResponse
)
router = APIRouter()

# Initialize controllers
health_controller = HealthController()
document_controller = DocumentController()
storage_controller = StorageController()
organization_controller = OrganizationController()
query_controller = QueryController()

@router.post("/api/query", response_model=QueryResponse)
async def execute_query(request: Request, request_data: QueryRequest):
    """Execute a query against specified targets."""
    return await query_controller.execute_query(request, request_data)

@router.get("/api/health", response_model=HealthResponse)
async def health():
    """Health check endpoint."""
    return health_controller.health_check()

@router.post("/api/generate-upload-url", response_model=GenerateUploadUrlResponse)
async def generate_upload_url(request: Request):
    """Generate a pre-signed upload URL for document upload."""
    return await document_controller.generate_upload_url(request)

@router.post("/api/index-doc", response_model=IndexDocResponse)
async def index_doc(request: Request):
    """Called after client uploads file to MinIO."""
    return await document_controller.index_document(request)

@router.post("/api/create-bucket", response_model=CreateBucketResponse)
async def create_organization_bucket(request: Request):
    """Create a MinIO bucket for an organization using organization ID from header."""
    return await storage_controller.create_organization_bucket(request)

@router.get("/api/buckets", response_model=ListBucketsResponse)
async def get_buckets():
    """List all available buckets in MinIO."""
    return storage_controller.list_buckets()

@router.get("/api/organizations/stats", response_model=OrganizationStats)
async def get_organization_stats():
    """Get statistics about active organizations in the multi-tenant pipeline."""
    return organization_controller.get_organization_stats()
