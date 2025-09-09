import re
import sqlite3
from haystack import component
from haystack.dataclasses import Document
from haystack_integrations.components.generators.ollama import OllamaGenerator


@component
class SQLGenerator:
    """
    A Haystack component that converts natural language questions to SQL queries using an LLM.
    
    This component takes a natural language question and database schema,
    then uses an Ollama model to generate a corresponding SQL query.
    """

    def __init__(self, model: str, base_url: str, schema: str = ""):
        """
        Initialize the SQL generator.
        
        Args:
            model: Ollama model name (e.g., "llama3.2", "mistral")
            base_url: Ollama server URL (e.g., "http://localhost:11434")
            schema: Database schema description for SQL generation
        """
        self.llm = OllamaGenerator(model=model, url=base_url)
        self.schema = schema

    @component.output_types(sql=str)
    def run(self, question: str) -> dict:
        """
        Generate SQL query from natural language question.
        
        Args:
            question: Natural language question to convert to SQL
            
        Returns:
            Dictionary containing the generated SQL query
        """
        prompt = (
            "You are a SQL expert. "
            "Given the following database schema, generate ONE valid SQL query "
            "to answer the user's question.\n\n"
            f"Schema:\n{self.schema}\n\n"
            f"Question: {question}\n\n"
            "Return only the SQL inside ```sql ... ```."
        )
        reply = self.llm.run(prompt=prompt)["replies"][0]

        # Extract SQL from code fence
        m = re.search(r"```sql\s+(.*?)```", reply, flags=re.S | re.I)
        sql = m.group(1).strip() if m else ""
        return {"sql": sql}


@component
class SQLQuery:
    """
    A Haystack component that executes SQL queries against a database.
    
    This component takes a SQL query string, executes it against a SQLite database,
    and returns the results wrapped as Haystack Documents for pipeline processing.
    """

    def __init__(self, conn_str: str):
        """
        Initialize the SQL query executor.
        
        Args:
            conn_str: SQLite database connection string/path
        """
        self.conn_str = conn_str

    @component.output_types(documents=list[Document])
    def run(self, query: str):
        """
        Execute SQL query against the database.
        
        Args:
            query: SQL query string to execute
            
        Returns:
            Dictionary containing query results wrapped as Documents
        """
        with sqlite3.connect(self.conn_str) as conn:
            cur = conn.execute(query)
            cols = [c[0] for c in cur.description] if cur.description else []
            rows = cur.fetchall()

        # Wrap results as a Haystack Document for pipeline processing
        content = f"SQL Results:\nColumns: {cols}\nRows: {rows[:20]}"
        return {"documents": [Document(content=content)]}
