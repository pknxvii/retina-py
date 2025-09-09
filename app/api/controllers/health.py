from app.api.controllers.base import BaseController
from app.api.models.responses import HealthResponse


class HealthController(BaseController):
    """Controller for health check operations."""
    
    def __init__(self):
        super().__init__()
    
    def health_check(self) -> HealthResponse:
        """Perform health check."""
        return HealthResponse()
