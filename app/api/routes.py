from fastapi import APIRouter, UploadFile
from app.dispatcher.celery_dispatcher import CeleryDispatcher

router = APIRouter()
dispatcher = CeleryDispatcher()

@router.get("/health")
async def health():
    return {"status": "healthy"}

@router.post("/upload-doc")
async def upload_doc():
    # content = await file.read()
    payload = {"doc_id": "123", "content": "content"}
    dispatcher.dispatch("index_document", payload)
    return {"status": "queued", "doc_id": "123"}
