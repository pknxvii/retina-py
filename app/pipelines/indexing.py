from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

def run_indexing_pipeline(doc_id: str, content: str):
    # TODO: setup Haystack pipeline
    logger.info(f"Running indexing pipeline for doc {doc_id}")
