from fastapi import APIRouter, UploadFile, Request, HTTPException
from app.dispatcher.celery_dispatcher import CeleryDispatcher
from app.utils.id_generator import generate_doc_id
from app.storage.minio_client import generate_presigned_upload_url, create_bucket, list_buckets
from app.pipelines.indexing import NativeHaystackPipeline
from app.config_loader import configuration

router = APIRouter()
dispatcher = CeleryDispatcher()

#TODO add pydantic model for the request response

@router.get("/health")
async def health():
    return {"status": "healthy"}


@router.post("/generate-upload-url")
async def generate_upload_url(request: Request):
    doc_id = generate_doc_id()

    header = request.headers
    user_id = header.get("X-User-Id")
    organization_id = header.get("X-Organization-Id")
    
    body = await request.json()
    doc_type = body["doc_type"] #pdf, txt, etc.
    object_name = f"{doc_id}.{doc_type}"
    object_path = f"{organization_id}/{user_id}/{object_name}"

    upload_url = generate_presigned_upload_url(object_name)

    return {
        "doc_id": doc_id,
        "upload_url": upload_url,
        "object_path": object_path,
    }

@router.post("/index-doc")
async def index_doc(request: Request):
    """Called after client uploads file to MinIO."""
    headers = request.headers
    user_id = headers.get("X-User-Id")
    organization_id = headers.get("X-Organization-Id")
    
    body = await request.json()
    doc_id = body["doc_id"]
    object_path = body["object_path"]
    
    dispatcher.dispatch("index_document", {"doc_id": doc_id, "object_path": object_path, "user_id": user_id, "organization_id": organization_id})
    return {"status": "Indexing dispatched", "doc_id": doc_id}

@router.post("/create-bucket")
async def create_organization_bucket(request: Request):
    """Create a MinIO bucket for an organization using organization ID from header."""
    headers = request.headers
    organization_id = headers.get("X-Organization-Id")
    
    if not organization_id:
        raise HTTPException(
            status_code=400, 
            detail="X-Organization-Id header is required"
        )
    
    # Sanitize organization ID for bucket name (MinIO bucket names have restrictions)
    tenancy_config = configuration["tenancy"]
    org_prefix = tenancy_config["organization_prefix"]
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
    
    return {
        "organization_id": organization_id,
        "bucket_name": result["bucket_name"],
        "status": result["status"],
        "message": result["message"]
    }

@router.get("/buckets")
async def get_buckets():
    """List all available buckets in MinIO."""
    buckets = list_buckets()
    return {"buckets": buckets}

@router.get("/organizations/stats")
async def get_organization_stats():
    """Get statistics about active organizations in the multi-tenant pipeline."""
    pipeline = NativeHaystackPipeline()
    stats = pipeline.get_organization_stats()
    return {
        "pipeline_instance_id": NativeHaystackPipeline.get_instance_id(),
        "multi_tenant_stats": stats
    }
