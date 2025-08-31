from celery import Celery
from app.config import Config
from app.pipelines.indexing import run_indexing_pipeline

celery_app = Celery("tasks", broker=Config.REDIS_URL)

@celery_app.task
def index_document(payload: dict):
    doc_id = payload["doc_id"]
    content = payload["content"]
    print(f"[Celery] Indexing doc {doc_id}")
    run_indexing_pipeline(doc_id, content)
