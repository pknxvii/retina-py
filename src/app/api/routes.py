from fastapi import APIRouter, UploadFile
from app.dispatcher.celery_dispatcher import CeleryDispatcher

router = APIRouter()
dispatcher = CeleryDispatcher()

@router.post("/upload-doc")
async def upload_doc(file: UploadFile):
    content = await file.read()
    payload = {"doc_id": "123", "content": content.decode()}
    dispatcher.dispatch("index_document", payload)
    return {"status": "queued", "doc_id": "123"}
