import re
from typing import List
from haystack import Pipeline, component
from haystack.components.routers import ConditionalRouter
from haystack.components.joiners import DocumentJoiner
from haystack_integrations.components.generators.ollama import OllamaGenerator
from haystack.components.builders import PromptBuilder
from haystack.dataclasses import Document

# Qdrant retriever and embedder imports
from haystack_integrations.components.retrievers.qdrant import QdrantEmbeddingRetriever
from haystack.components.embedders import SentenceTransformersTextEmbedder
from app.storage.document_store_manager import DocumentStoreManager


# ----------------------------------------------------------------------
# SQL Generator Component - converts natural language to SQL
# ----------------------------------------------------------------------
@component
class SQLGenerator:
    def __init__(self, model: str, base_url: str, schema: str = ""):
        self.llm = OllamaGenerator(model=model, url=base_url)
        self.schema = schema

    @component.output_types(sql=str)
    def run(self, question: str) -> dict:
        prompt = (
            "You are a SQL expert. "
            "Given the following database schema, generate ONE valid SQL query "
            "to answer the user's question.\n\n"
            f"Schema:\n{self.schema}\n\n"
            f"Question: {question}\n\n"
            "Return only the SQL inside ```sql ... ```."
        )
        reply = self.llm.run(prompt=prompt)["replies"][0]

        # extract SQL from code fence
        m = re.search(r"```sql\s+(.*?)```", reply, flags=re.S | re.I)
        sql = m.group(1).strip() if m else ""
        return {"sql": sql}


# ----------------------------------------------------------------------
# Simple SQL component wrapper (you can move this to app/utils/sql_client.py)
# ----------------------------------------------------------------------
import sqlite3

@component
class SQLQuery:
    def __init__(self, conn_str: str):
        self.conn_str = conn_str

    @component.output_types(documents=list[Document])
    def run(self, query: str):
        """Run raw SQL string against DB and wrap results as Documents."""
        with sqlite3.connect(self.conn_str) as conn:
            cur = conn.execute(query)
            cols = [c[0] for c in cur.description] if cur.description else []
            rows = cur.fetchall()

        # wrap as a haystack Document so it can flow into joiner
        content = f"SQL Results:\nColumns: {cols}\nRows: {rows[:20]}"
        return {"documents": [Document(content=content)]}


class QueryPipeline:
    def __init__(self, db_conn_str: str, db_schema: str = "", llm_config: dict = None, qdrant_config: dict = None, embedder_config: dict = None, retriever_config: dict = None):
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
        self.pipeline = self.build_query_pipeline(db_conn_str=db_conn_str, db_schema=db_schema)

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
        sql_exec = SQLQuery(conn_str=db_conn_str)
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

    def run_query(self, query: str, targets: List[str], organization_id: str = None, user_id: str = None) -> str:
        # Set up the document store and retriever for the organization if needed
        if organization_id and "docstore" in targets:
            store_manager = DocumentStoreManager()
            document_store = store_manager.get_document_store(organization_id)
            
            # Create metadata filters for Haystack QdrantEmbeddingRetriever
            metadata_filters = {
                "operator": "AND",
                "conditions": [
                    {
                        "field": "meta.organization_id",
                        "operator": "==",
                        "value": organization_id
                    }
                ]
            }
            
            # Add user filter if user_id is provided
            if user_id:
                metadata_filters["conditions"].append({
                    "field": "meta.user_id",
                    "operator": "==",
                    "value": user_id
                })
            
            # Create and add the retriever component dynamically
            doc_retriever = QdrantEmbeddingRetriever(
                document_store=document_store,
                top_k=self.retriever_config.get("top_k"),
                #filters=metadata_filters
                #TODO: With metadata filters, query is not working. Need to fix.
            )
            
            # Add the retriever to the pipeline if not already added
            if "doc_retriever" not in self.pipeline.graph.nodes():
                self.pipeline.add_component("doc_retriever", doc_retriever)
                # Connect the retriever in the pipeline
                self.pipeline.connect("query_embedder.embedding", "doc_retriever.query_embedding")
                # Always connect to joiner - joiner can handle single input
                self.pipeline.connect("doc_retriever", "joiner.documents")
            else:
                # Update existing retriever
                self.pipeline.get_component("doc_retriever").document_store = document_store
                self.pipeline.get_component("doc_retriever").filters = metadata_filters
            
        result = self.pipeline.run({
            "router": {"targets": targets, "query": query},
            "prompt_builder": {"query": query},
        })
        return result["llm"]["replies"][0]


