from celery.utils.log import get_task_logger
import structlog

logger = structlog.get_logger()

def run_indexing_pipeline(doc_id: str, content: str):
    # TODO: setup Haystack pipeline
    logger.info(f"[Pipeline] Running indexing pipeline for doc {doc_id}")