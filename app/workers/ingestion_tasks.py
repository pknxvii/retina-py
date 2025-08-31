from celery import Celery
from celery.utils.log import get_task_logger
from app.config_loader import configuration
from app.pipelines.indexing import run_indexing_pipeline
import time

celery_app = Celery(
    "tasks", 
    broker=configuration["celery"]["broker_url"],
    backend=configuration["celery"]["result_backend"]
)

# Configure celery from YAML
celery_config = configuration["celery"]
# celery_app.conf.update(key=value) #can be used to update celery config

logger = get_task_logger(__name__)

@celery_app.task(bind=True)
def index_document(self, payload: dict):
    # Use both logger and print for debugging
    
    doc_id = payload["doc_id"]
    content = payload["content"]
    
    logger.info(f"[Celery] Started indexing document {doc_id}")
    logger.info(f"[Celery] Content length: {len(content)} characters")
    
    try:
        # Simulating some processing time
        time.sleep(10)
        run_indexing_pipeline(doc_id, content)
        
        logger.info(f"[Celery] Successfully indexed document {doc_id}")
        
        result = {"status": "success", "doc_id": doc_id}
        return result
        
    except Exception as exc:
        logger.error(f"[Celery] Failed to index document {doc_id}: {exc}")
        raise self.retry(exc=exc, countdown=60, max_retries=3)
