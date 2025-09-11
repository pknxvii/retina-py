from celery import Celery
from celery.utils.log import get_task_logger
from app.config_loader import configuration
from app.pipelines.indexing import IndexingPipelinesFactory
import time
import multiprocessing

# Fix for macOS fork issues with ML libraries
if hasattr(multiprocessing, 'set_start_method'):
    try:
        multiprocessing.set_start_method('spawn', force=True)
    except RuntimeError:
        pass  # Already set

celery_app = Celery(
    "tasks", 
    broker=configuration["celery"]["broker_url"],
    backend=configuration["celery"]["result_backend"]
)

# Configure celery from YAML and set worker pool to avoid fork issues
#TODO: Need to fix multiprocessing issues
celery_config = configuration["celery"]
celery_app.conf.update(
    worker_pool='solo',  # Use solo pool to avoid multiprocessing issues
    worker_concurrency=1,  # Single worker to avoid conflicts
    worker_prefetch_multiplier=1,  # Process one task at a time
)

logger = get_task_logger(__name__)

@celery_app.task(bind=True)
def index_document(self, payload: dict):
    # Use both logger and print for debugging
    logger.debug(f"[Celery] Indexing document with payload: {payload}")
    doc_id = payload["doc_id"]
    object_path = payload["object_path"]
    user_id = payload["user_id"]
    organization_id = payload["organization_id"]
    
    logger.info(f"[Celery] Started indexing document {doc_id}")
    logger.info(f"[Celery] Object path: {object_path}")
    
    try:
        # Create pipeline factory instance and run indexing
        pipeline_factory = IndexingPipelinesFactory()
        instance_id = IndexingPipelinesFactory.get_instance_id()
        logger.info(f"[Celery] Using pipeline factory instance ID: {instance_id}")
        
        result = pipeline_factory.run_indexing_pipeline(doc_id, object_path, user_id, organization_id)
        
        logger.info(f"[Celery] Successfully indexed document {doc_id}")
        
        return result
        
    except Exception as exc:
        logger.error(f"[Celery] Failed to index document {doc_id}: {exc}")
        raise self.retry(exc=exc, countdown=60, max_retries=3)
