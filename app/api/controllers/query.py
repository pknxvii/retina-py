from fastapi import Request
from app.api.controllers.base import BaseController
from app.api.services.query import QueryService
from app.api.models.requests import QueryRequest
from app.api.models.responses import QueryResponse


class QueryController(BaseController):
    """Controller for query-related operations."""
    
    def __init__(self):
        super().__init__()
        self.query_service = QueryService()
    
    async def execute_query(self, request: Request, request_data: QueryRequest) -> QueryResponse:
        """Execute a query against specified targets."""
        headers = self.extract_headers(request)
        return self.query_service.execute_query(
            request_data, 
            organization_id=headers.organization_id,
            user_id=headers.user_id
        )
