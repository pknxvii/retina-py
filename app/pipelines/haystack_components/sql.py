import re
import sqlite3
from typing import List, Set
from haystack import component
from haystack.dataclasses import Document
from haystack_integrations.components.generators.ollama import OllamaGenerator


class SQLSafetyValidator:
    """
    A validator class to ensure SQL queries are safe and non-destructive.
    
    This class uses both rule-based and LLM-based approaches to validate
    SQL queries before execution, preventing data modification or schema changes.
    """
    
    def __init__(self, llm_model: str = None, llm_base_url: str = None):
        # Allowed SQL operations (whitelist approach for quick checks)
        self.allowed_operations: Set[str] = {
            "SELECT", "WITH", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "PRAGMA"
        }
        
        # Dangerous SQL keywords (blacklist approach for quick checks)
        self.dangerous_keywords: Set[str] = {
            "DROP", "DELETE", "UPDATE", "INSERT", "TRUNCATE", "ALTER", 
            "CREATE", "REPLACE", "MERGE", "UPSERT", "ATTACH", "DETACH",
            "VACUUM", "REINDEX", "ANALYZE"
        }
        
        # Initialize LLM for advanced safety checking if provided
        self.use_llm = llm_model and llm_base_url
        if self.use_llm:
            self.llm = OllamaGenerator(model=llm_model, url=llm_base_url)
        
        # Additional dangerous patterns for rule-based checking
        self.dangerous_patterns: List[str] = [
            r";\s*(DROP|DELETE|UPDATE|INSERT|TRUNCATE|ALTER|CREATE)",  # Multiple statements
            r"--.*?(DROP|DELETE|UPDATE|INSERT|TRUNCATE|ALTER|CREATE)",  # Commented dangerous ops
            r"/\*.*?(DROP|DELETE|UPDATE|INSERT|TRUNCATE|ALTER|CREATE).*?\*/",  # Block commented ops
            r"EXEC\s*\(",  # Stored procedure execution
            r"EXECUTE\s*\(",  # Execute statements
            r"xp_",  # Extended stored procedures (SQL Server)
            r"sp_",  # System stored procedures
        ]
    
    def is_safe_query(self, query: str) -> tuple[bool, str]:
        """
        Validate if a SQL query is safe to execute using both rule-based and LLM-based approaches.
        
        Args:
            query: SQL query string to validate
            
        Returns:
            Tuple of (is_safe: bool, reason: str)
        """
        if not query or not query.strip():
            return False, "Empty query provided"
        
        # First, do quick rule-based checks for obvious violations
        rule_based_result = self._rule_based_safety_check(query)
        if not rule_based_result[0]:
            return rule_based_result
        
        # If LLM is available, use it for more sophisticated analysis
        if self.use_llm:
            return self._llm_based_safety_check(query)
        
        # If no LLM, rely on rule-based checks only
        return rule_based_result
    
    def _rule_based_safety_check(self, query: str) -> tuple[bool, str]:
        """
        Perform rule-based safety validation using patterns and keywords.
        
        Args:
            query: SQL query string to validate
            
        Returns:
            Tuple of (is_safe: bool, reason: str)
        """
        # Normalize query for analysis
        normalized_query = query.upper().strip()
        normalized_query = re.sub(r'\s+', ' ', normalized_query)
        
        # Check if query starts with allowed operations
        first_word = normalized_query.split()[0] if normalized_query.split() else ""
        if first_word not in self.allowed_operations:
            return False, f"Query must start with allowed operations: {', '.join(self.allowed_operations)}. Found: {first_word}"
        
        # Check for dangerous keywords (but be smart about string literals)
        if self._has_dangerous_keywords_in_context(normalized_query):
            return False, "Dangerous keyword detected in executable context"
        
        # Check for dangerous patterns using regex
        for pattern in self.dangerous_patterns:
            if re.search(pattern, normalized_query, re.IGNORECASE | re.DOTALL):
                return False, f"Dangerous pattern detected matching: {pattern}"
        
        # Additional checks for SQL injection attempts
        if self._check_sql_injection_patterns(normalized_query):
            return False, "Potential SQL injection pattern detected"
        
        return True, "Query passed rule-based safety checks"
    
    def _llm_based_safety_check(self, query: str) -> tuple[bool, str]:
        """
        Use LLM to perform sophisticated safety analysis of SQL queries.
        
        Args:
            query: SQL query string to validate
            
        Returns:
            Tuple of (is_safe: bool, reason: str)
        """
        safety_prompt = f"""
        You are a SQL security expert analyzing queries for a read-only reporting system.

        CLASSIFICATION RULES:
        SAFE queries - READ-ONLY operations that do NOT modify data or schema:
        - SELECT statements (including JOINs, subqueries, aggregations, window functions)
        - WITH clauses (Common Table Expressions)
        - SHOW, DESCRIBE, DESC commands
        - EXPLAIN, EXPLAIN QUERY PLAN statements
        - PRAGMA read-only operations

        UNSAFE queries - ANYTHING that modifies data or schema:
        - INSERT, UPDATE, DELETE statements
        - CREATE, DROP, ALTER statements (tables, indexes, etc.)
        - TRUNCATE, REPLACE statements
        - Multiple statements separated by semicolons
        - SQL injection patterns (like '; DROP TABLE, UNION attacks)
        - Stored procedure calls (EXEC, EXECUTE)

        IMPORTANT CONTEXT:
        - Keywords like "UPDATE", "DELETE", "CREATE" appearing in string literals or column names are SAFE
        - Complex SELECT queries with JOINs and aggregations are SAFE
        - Subqueries and CTEs are SAFE as long as they only use SELECT
        - Performance is not a safety concern - focus only on data/schema modification

        SQL Query to analyze:
        ```sql
        {query}
        ```

        Respond with EXACTLY:
        "SAFE: [reason]" OR "UNSAFE: [reason]"

        Response:"""

        try:
            response = self.llm.run(prompt=safety_prompt)["replies"][0].strip()
            
            if response.startswith("SAFE:"):
                reason = response[5:].strip()
                return True, f"LLM validation passed: {reason}"
            elif response.startswith("UNSAFE:"):
                reason = response[7:].strip()
                return False, f"LLM validation failed: {reason}"
            else:
                # If LLM doesn't respond in expected format, be conservative
                return False, f"LLM validation inconclusive: {response}"
                
        except Exception as e:
            # If LLM fails, fall back to rule-based validation
            return self._rule_based_safety_check(query)
    
    def _has_dangerous_keywords_in_context(self, query: str) -> bool:
        """
        Check for dangerous keywords but ignore them when they appear in string literals.
        
        Args:
            query: Normalized SQL query string
            
        Returns:
            True if dangerous keywords are found in executable context
        """
        # Remove string literals to avoid false positives
        # Handle both single and double quotes
        query_without_strings = re.sub(r"'[^']*'", " 'STRING' ", query)
        query_without_strings = re.sub(r'"[^"]*"', ' "STRING" ', query_without_strings)
        
        # Check for dangerous keywords in the cleaned query
        # Use word boundaries to avoid partial matches
        for keyword in self.dangerous_keywords:
            pattern = r'\b' + re.escape(keyword) + r'\b'
            if re.search(pattern, query_without_strings, re.IGNORECASE):
                return True
        return False
    
    def _check_sql_injection_patterns(self, query: str) -> bool:
        """Check for common SQL injection patterns."""
        # Remove string literals first to avoid false positives
        query_without_strings = re.sub(r"'[^']*'", " 'STRING' ", query)
        query_without_strings = re.sub(r'"[^"]*"', ' "STRING" ', query_without_strings)
        
        injection_patterns = [
            r"'\s*OR\s*'",  # ' OR ' (string concatenation)
            r"'\s*AND\s*'",  # ' AND ' (string concatenation)
            r"'\s*;\s*",  # '; (semicolon after quote)
            r"'\s*OR\s+1\s*=\s*1",  # ' OR 1=1
            r"'\s*OR\s+TRUE",  # ' OR TRUE  
            r"'\s*OR\s+'[^']*'\s*=\s*'[^']*'",  # ' OR 'a'='a'
            r"0\s*=\s*0",  # 0=0 (always true)
            r"NULL\s*IS\s*NULL",  # NULL IS NULL (always true)
            # UNION injection - but exclude legitimate CTEs by checking context
            r"[;]\s*UNION\s+(ALL\s+)?SELECT",  # ; UNION (ALL) SELECT - semicolon separated injection
            r"'\s*UNION\s+(ALL\s+)?SELECT",  # ' UNION (ALL) SELECT - string break injection
        ]
        
        for pattern in injection_patterns:
            if re.search(pattern, query_without_strings, re.IGNORECASE):
                return True
        return False


@component
class SQLGenerator:
    """
    A Haystack component that converts natural language questions to SQL queries using an LLM.
    
    This component takes a natural language question and database schema,
    then uses an Ollama model to generate a corresponding SQL query.
    Includes safety validation to prevent destructive operations.
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
        # Initialize safety validator with LLM for advanced safety checking
        self.safety_validator = SQLSafetyValidator(llm_model=model, llm_base_url=base_url)

    @component.output_types(sql=str)
    def run(self, question: str) -> dict:
        """
        Generate SQL query from natural language question with safety validation.
        
        Args:
            question: Natural language question to convert to SQL
            
        Returns:
            Dictionary containing the generated SQL query
            
        Raises:
            ValueError: If the generated query is deemed unsafe
        """
        prompt = (
            "You are a SQL expert. "
            "Given the following database schema, generate ONE valid SQL query "
            "to answer the user's question.\n\n"
            "IMPORTANT SAFETY RULES:\n"
            "- ONLY generate SELECT, WITH, SHOW, DESCRIBE, DESC, EXPLAIN, or PRAGMA statements\n"
            "- NEVER generate INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE, or any data modification queries\n"
            "- Focus on data retrieval and analysis only\n"
            "- Do not include comments or multiple statements\n\n"
            f"Schema:\n{self.schema}\n\n"
            f"Question: {question}\n\n"
            "Return only the SQL inside ```sql ... ```."
        )
        reply = self.llm.run(prompt=prompt)["replies"][0]

        # Extract SQL from code fence or fallback to the entire reply if no fence
        m = re.search(r"```sql\s+(.*?)```", reply, flags=re.S | re.I)
        if m:
            sql = m.group(1).strip()
        else:
            # If no SQL code fence, try to extract SQL from the reply
            # Look for common SQL patterns and clean up the response
            sql = reply.strip()
            # Remove any leading/trailing text that's not SQL
            sql = re.sub(r'^.*?(?=SELECT|WITH|SHOW|DESCRIBE|DESC|EXPLAIN|PRAGMA)', '', sql, flags=re.IGNORECASE | re.DOTALL)
            sql = re.sub(r';.*$', ';', sql, flags=re.DOTALL)  # Remove text after semicolon
            sql = sql.strip()
        
        # Validate the generated SQL for safety
        is_safe, reason = self.safety_validator.is_safe_query(sql)
        if not is_safe:
            raise ValueError(f"Generated SQL query failed safety validation: {reason}. Query: {sql}")
        
        return {"sql": sql}


@component
class SQLQuery:
    """
    A Haystack component that executes SQL queries against a database.
    
    This component takes a SQL query string, executes it against a SQLite database,
    and returns the results wrapped as Haystack Documents for pipeline processing.
    Includes safety validation to prevent destructive operations.
    """

    def __init__(self, conn_str: str, llm_model: str = None, llm_base_url: str = None):
        """
        Initialize the SQL query executor.
        
        Args:
            conn_str: SQLite database connection string/path
            llm_model: Optional LLM model for advanced safety checking
            llm_base_url: Optional LLM base URL for advanced safety checking
        """
        self.conn_str = conn_str
        # Initialize safety validator with optional LLM for advanced safety checking
        self.safety_validator = SQLSafetyValidator(llm_model=llm_model, llm_base_url=llm_base_url)

    @component.output_types(documents=list[Document])
    def run(self, query: str):
        """
        Execute SQL query against the database with safety validation.
        
        Args:
            query: SQL query string to execute
            
        Returns:
            Dictionary containing query results wrapped as Documents
            
        Raises:
            ValueError: If the query is deemed unsafe for execution
        """
        # Validate query safety before execution
        is_safe, reason = self.safety_validator.is_safe_query(query)
        if not is_safe:
            raise ValueError(f"SQL query failed safety validation: {reason}. Query: {query}")
        
        try:
            with sqlite3.connect(self.conn_str) as conn:
                # Set a timeout to prevent long-running queries
                conn.execute("PRAGMA busy_timeout = 30000")  # 30 second timeout
                
                cur = conn.execute(query)
                cols = [c[0] for c in cur.description] if cur.description else []
                rows = cur.fetchall()

            # Limit the number of rows returned to prevent memory issues
            max_rows = 1000
            if len(rows) > max_rows:
                rows = rows[:max_rows]
                content = f"SQL Results (showing first {max_rows} of {len(rows)} rows):\nColumns: {cols}\nRows: {rows}"
            else:
                content = f"SQL Results:\nColumns: {cols}\nRows: {rows}"
                
            return {"documents": [Document(content=content)]}
            
        except sqlite3.Error as e:
            raise ValueError(f"Database error executing query: {str(e)}. Query: {query}")
        except Exception as e:
            raise ValueError(f"Unexpected error executing query: {str(e)}. Query: {query}")
