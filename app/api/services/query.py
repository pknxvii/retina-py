from app.pipelines.query import QueryPipelinesFactory
from app.api.models.requests import QueryRequest
from app.api.models.responses import QueryResponse


class QueryService:
    """Service layer for query operations with multi-tenant support."""
    
    def __init__(self):
        # Use the singleton factory for managing query pipelines
        self.query_factory = QueryPipelinesFactory()
    
    def execute_query(self, request_data: QueryRequest, organization_id: str = None, user_id: str = None) -> QueryResponse:
        """Execute a query against specified targets with proper multi-tenant isolation."""
        try:
            # Determine which pipeline to use based on targets
            targets = request_data.targets
            pipeline = self.query_factory.get_organization_pipeline(organization_id)
            
            answer = pipeline.run_query(
                query=request_data.query,
                targets=targets,
                organization_id=organization_id,
                user_id=user_id
            )
            
            return QueryResponse(
                answer=answer,
                sources=None  # TODO: Extract sources from pipeline result if available
            )
        except Exception as e:
            return QueryResponse(
                answer=f"Error executing query: {str(e)}",
                sources=None
            )
