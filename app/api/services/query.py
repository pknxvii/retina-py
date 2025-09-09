from app.pipelines.query import QueryPipeline
from app.api.models.requests import QueryRequest
from app.api.models.responses import QueryResponse
from app.config_loader import configuration


class QueryService:
    """Service layer for query operations."""
    
    def __init__(self):
        # Initialize query pipeline with database, LLM, Qdrant, and embedder configuration
        db_config = configuration.get("database", {})
        llm_config = configuration.get("llm", {})
        qdrant_config = configuration.get("qdrant", {})
        haystack_config = configuration.get("haystack", {})
        query_config = configuration.get("query", {})
        
        # Validate required configuration
        if not llm_config:
            raise ValueError("LLM configuration is missing from config.yaml")
        if not qdrant_config:
            raise ValueError("Qdrant configuration is missing from config.yaml")
        if not haystack_config.get("embedder"):
            raise ValueError("Embedder configuration is missing from config.yaml")
        
        db_conn_str = db_config.get("connection_string")
        if not db_conn_str:
            raise ValueError("Database connection string is missing from config.yaml")
        
        db_schema = db_config.get("schema", "")  # Database schema for SQL generation
        
        # LLM configuration for Ollama
        llm_settings = {
            "model": llm_config["model"],
            "base_url": llm_config["base_url"]
        }
        
        # Qdrant configuration
        qdrant_settings = {
            "url": qdrant_config["url"],
            "index": qdrant_config["index"]
        }
        
        # Embedder configuration
        embedder_settings = {
            "model": haystack_config["embedder"]["model"]
        }
        
        # Retriever configuration
        retriever_settings = {
            "top_k": query_config.get("retriever", {}).get("top_k", 10)
        }
        
        self.query_pipeline = QueryPipeline(
            db_conn_str=db_conn_str, 
            db_schema=db_schema,
            llm_config=llm_settings,
            qdrant_config=qdrant_settings,
            embedder_config=embedder_settings,
            retriever_config=retriever_settings
        )
    
    def execute_query(self, request_data: QueryRequest, organization_id: str = None, user_id: str = None) -> QueryResponse:
        """Execute a query against specified targets."""
        try:
            answer = self.query_pipeline.run_query(
                query=request_data.query,
                targets=request_data.targets,
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
