from typing import List, Dict
from haystack import Pipeline
from haystack.components.routers import ConditionalRouter
from haystack.components.joiners import DocumentJoiner
from haystack_integrations.components.generators.ollama import OllamaGenerator
from haystack.components.builders import PromptBuilder

# Qdrant retriever and embedder imports
from haystack_integrations.components.retrievers.qdrant import QdrantEmbeddingRetriever
from haystack.components.embedders import SentenceTransformersTextEmbedder

# Import custom SQL components
from app.pipelines.haystack_components import SQLGenerator, SQLQuery
from app.storage.document_store_manager import DocumentStoreManager

import structlog
from app.config_loader import configuration


class QueryPipeline:
    def __init__(self, db_conn_str: str, db_schema: str = "", llm_config: dict = None, qdrant_config: dict = None, embedder_config: dict = None, retriever_config: dict = None, organization_id: str = None):
        
        if not llm_config:
            raise ValueError("llm_config is required")
        if not qdrant_config:
            raise ValueError("qdrant_config is required")
        if not embedder_config:
            raise ValueError("embedder_config is required")
            
        self.db_schema = db_schema
        self.llm_config = llm_config
        self.qdrant_config = qdrant_config
        self.embedder_config = embedder_config
        self.retriever_config = retriever_config or {"top_k": 10}
        self.organization_id = organization_id
        
        # Build pipeline with organization-specific components if needed
        self.pipeline = self.build_query_pipeline(db_conn_str=db_conn_str, db_schema=db_schema)
        
        # Pre-configure document store and retriever for this organization if specified
        if organization_id:
            self._setup_organization_retriever()

    def build_query_pipeline(self, db_conn_str: str, db_schema: str = ""):
        # Logic to build the query pipeline
        pipe = Pipeline()

        # Router: decides which branches to activate
        routes = [
            {
                "condition": '{{ "docstore" in targets }}',
                "output": "{{ query }}",
                "output_name": "docstore",
                "output_type": str
            },
            {
                "condition": '{{ "sql" in targets }}',
                "output": "{{ query }}",
                "output_name": "sql", 
                "output_type": str
            }
        ]
        router = ConditionalRouter(routes=routes)
        pipe.add_component("router", router)

        # Docstore branch - Query embedder only (retriever will be added dynamically)
        query_embedder = SentenceTransformersTextEmbedder(
            model=self.embedder_config["model"]
        )
        pipe.add_component("query_embedder", query_embedder)
        
        # Note: doc_retriever will be added dynamically in run_query method

        # SQL branch with NL->SQL generation
        sql_generator = SQLGenerator(
            model=self.llm_config["model"],
            base_url=self.llm_config["base_url"],
            schema=db_schema
        )
        sql_exec = SQLQuery(
            conn_str=db_conn_str,
            llm_model=self.llm_config["model"],
            llm_base_url=self.llm_config["base_url"]
        )
        pipe.add_component("sql_generator", sql_generator)
        pipe.add_component("sql_exec", sql_exec)

        # Joiner + Prompt Builder + Final LLM
        joiner = DocumentJoiner()
        prompt_builder = PromptBuilder(
            template="""
                Based on the following information, please answer the question:

                Context:
                {% for doc in documents %}
                {{ doc.content }}
                {% endfor %}

                Question: {{ query }}

                Answer:
                """,
            required_variables=["documents", "query"]
        )
        generator = OllamaGenerator(
            model=self.llm_config["model"],
            url=self.llm_config["base_url"]
        )

        pipe.add_component("joiner", joiner)
        pipe.add_component("prompt_builder", prompt_builder)
        pipe.add_component("llm", generator)

        # Wiring (doc_retriever connections will be added dynamically)
        pipe.connect("router.docstore", "query_embedder.text")
        pipe.connect("router.sql", "sql_generator.question")

        # Connect SQL generator to SQL executor
        pipe.connect("sql_generator.sql", "sql_exec.query")

        pipe.connect("sql_exec", "joiner.documents")
        pipe.connect("joiner", "prompt_builder.documents")
        pipe.connect("prompt_builder", "llm")

        return pipe

    def _setup_organization_retriever(self):
        """Setup organization-specific document retriever for this pipeline."""
        if not self.organization_id:
            return
            
        store_manager = DocumentStoreManager()
        document_store = store_manager.get_document_store(self.organization_id)
        
        # Create metadata filters for this organization
        metadata_filters = {
            "operator": "AND",
            "conditions": [
                {
                    "field": "meta.organization_id",
                    "operator": "==",
                    "value": self.organization_id
                }
            ]
        }

        # Create and add the retriever component
        doc_retriever = QdrantEmbeddingRetriever(
            document_store=document_store,
            top_k=self.retriever_config.get("top_k"),
            filters=metadata_filters
        )
        
        # Add the retriever to the pipeline
        self.pipeline.add_component("doc_retriever", doc_retriever)
        # Connect the retriever in the pipeline
        self.pipeline.connect("query_embedder.embedding", "doc_retriever.query_embedding")
        # Connect to joiner
        self.pipeline.connect("doc_retriever", "joiner.documents")

    def run_query(self, query: str, targets: List[str], organization_id: str = None, user_id: str = None) -> str:
        """Execute query on this pipeline instance."""
        # For organization-specific pipelines, we don't need to dynamically modify anything
        # The pipeline is already configured for the specific organization
        
        # TODO: Implement user-level filtering when needed
        # For now, organization-level isolation is the primary concern
        
        result = self.pipeline.run({
            "router": {"targets": targets, "query": query},
            "prompt_builder": {"query": query}
        })
        return result["llm"]["replies"][0]


class QueryPipelinesFactory:
    """Multi-tenant Factory for Query pipelines - Singleton implementation
    
    This factory creates and manages separate Query pipelines for each organization,
    enabling data isolation while sharing computational resources like ML models.
    """
    
    _instance = None
    _initialized = False
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        # Only initialize once, even if __init__ is called multiple times
        if not self._initialized:
            self.logger = structlog.get_logger(__name__)
            self.logger.info("[Query Factory] Initializing Multi-tenant Query Pipelines Factory")
            
            # Multi-tenant storage: pipelines per organization
            self._query_pipelines = {}  # org_id -> QueryPipeline
            self._shared_pipeline = None  # Shared pipeline for non-tenant specific operations (SQL)
            
            # Load configuration once
            self.db_config = configuration.get("database", {})
            self.llm_config = configuration.get("llm", {})
            self.qdrant_config = configuration.get("qdrant", {})
            self.haystack_config = configuration.get("haystack", {})
            self.query_config = configuration.get("query", {})
            
            # Validate required configuration
            self._validate_configuration()
            
            # Create shared pipeline for SQL queries and other non-tenant operations
            self._shared_pipeline = self._create_pipeline_for_organization(None)
            
            # Mark as initialized to prevent re-initialization
            QueryPipelinesFactory._initialized = True
        else:
            self.logger = structlog.get_logger(__name__)
            self.logger.debug("[Query Factory] Returning existing Multi-tenant Query Factory instance")
    
    def _validate_configuration(self):
        """Validate required configuration parameters."""
        if not self.llm_config:
            raise ValueError("LLM configuration is missing from config.yaml")
        if not self.qdrant_config:
            raise ValueError("Qdrant configuration is missing from config.yaml")
        if not self.haystack_config.get("embedder"):
            raise ValueError("Embedder configuration is missing from config.yaml")
        
        db_conn_str = self.db_config.get("connection_string")
        if not db_conn_str:
            raise ValueError("Database connection string is missing from config.yaml")
    
    def _create_pipeline_for_organization(self, organization_id: str = None) -> QueryPipeline:
        """Create a pipeline instance for a specific organization."""
        db_schema = self.db_config.get("schema", "")
        
        # LLM configuration for Ollama
        llm_settings = {
            "model": self.llm_config["model"],
            "base_url": self.llm_config["base_url"]
        }
        
        # Qdrant configuration
        qdrant_settings = {
            "url": self.qdrant_config["url"],
            "index": self.qdrant_config["index"]
        }
        
        # Embedder configuration
        embedder_settings = {
            "model": self.haystack_config["embedder"]["model"]
        }
        
        # Retriever configuration
        retriever_settings = {
            "top_k": self.query_config.get("retriever", {}).get("top_k", 10)
        }
        
        return QueryPipeline(
            db_conn_str=self.db_config.get("connection_string"),
            db_schema=db_schema,
            llm_config=llm_settings,
            qdrant_config=qdrant_settings,
            embedder_config=embedder_settings,
            retriever_config=retriever_settings,
            organization_id=organization_id
        )
    
    def get_shared_pipeline(self) -> QueryPipeline:
        """Get the shared pipeline for non-tenant specific operations (like SQL queries)."""
        self.logger.info("[Query Factory] Returning shared pipeline for non-tenant operations")
        return self._shared_pipeline
    
    def get_organization_pipeline(self, organization_id: str) -> QueryPipeline:
        """Get or create query pipeline for a specific organization."""
        if organization_id not in self._query_pipelines:
            self.logger.info(f"[Query Factory] Creating query pipeline for org: {organization_id}")
            self._query_pipelines[organization_id] = self._create_pipeline_for_organization(organization_id)
        else:
            self.logger.info(f"[Query Factory] Reusing existing query pipeline with id: {id(self._query_pipelines[organization_id])} for org: {organization_id}")
        
        return self._query_pipelines[organization_id]
    
    @classmethod
    def get_instance_id(cls):
        """Get the instance ID for debugging singleton behavior."""
        if cls._instance is not None:
            return id(cls._instance)
        return None
    
    def get_organization_stats(self):
        """Get statistics about active query pipelines."""
        return {
            "total_organizations": len(self._query_pipelines),
            "organizations": list(self._query_pipelines.keys()),
            "active_query_pipelines": len(self._query_pipelines),
            "has_shared_pipeline": self._shared_pipeline is not None,
            "query_factory_instance_id": id(self)
        }
    
    def clear_organization_pipeline(self, organization_id: str):
        """Clear/remove pipeline for a specific organization (useful for cleanup)."""
        if organization_id in self._query_pipelines:
            self.logger.info(f"[Query Factory] Clearing query pipeline for org: {organization_id}")
            del self._query_pipelines[organization_id]
    
    def clear_all_pipelines(self):
        """Clear all organization-specific pipelines (useful for cleanup/testing)."""
        self.logger.info("[Query Factory] Clearing all organization-specific query pipelines")
        self._query_pipelines.clear()
