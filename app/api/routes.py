from fastapi import APIRouter, UploadFile, Request
from app.dispatcher.celery_dispatcher import CeleryDispatcher
from app.utils.id_generator import generate_doc_id
from app.storage.minio_client import generate_presigned_upload_url

router = APIRouter()
dispatcher = CeleryDispatcher()

@router.get("/health")
async def health():
    return {"status": "healthy"}


@router.post("/generate-upload-url")
async def generate_upload_url():
    doc_id = generate_doc_id()
    object_name = f"{doc_id}.pdf"   # you can adjust by file type later
    object_path = f"{configuration['minio']['bucket']}/{object_name}"

    upload_url = generate_presigned_upload_url(object_name)

    return {
        "doc_id": doc_id,
        "upload_url": upload_url,
        "object_path": object_path,
    }

@router.post("/index-doc")
async def index_doc(request: Request):
    """Called after client uploads file to MinIO."""
    body = await request.json()
    doc_id = body["doc_id"]
    object_path = body["object_path"]
    doc_type = body["doc_type"] #PDF, DOCX, etc.
    
    dispatcher.dispatch("index_document", {"doc_id": doc_id, "object_path": object_path, "doc_type": doc_type})
    return {"status": "Indexing dispatched", "doc_id": doc_id}
