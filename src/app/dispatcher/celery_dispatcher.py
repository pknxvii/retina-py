from .base import TaskDispatcher
from app.workers.ingestion_tasks import index_document

class CeleryDispatcher(TaskDispatcher):
    def dispatch(self, task_name: str, payload: dict) -> None:
        if task_name == "index_document":
            index_document.delay(payload)
        else:
            raise ValueError(f"Unknown task: {task_name}")
