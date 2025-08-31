from abc import ABC, abstractmethod
from typing import Any, Dict

class TaskDispatcher(ABC):
    @abstractmethod
    def dispatch(self, task_name: str, payload: Dict[str, Any]) -> None:
        """Send a task for async processing"""
        pass
