"""
NL2SQL AI Agent for Stock Portfolio Analytics
Converts natural language queries to SQL using Google Gemini 2.5 Flash
Includes LangChain tools for file I/O, SQL execution, and CSV export
"""

import os
import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
from functools import wraps
import re
import sqlparse

# Google Gemini
import google.generativeai as genai

# LangChain imports
# Note: tool decorator moved in LangChain 0.1.0+ to langchain_core.tools
try:
    from langchain_core.tools import tool
except ImportError:
    try:
        from langchain.tools import tool
    except ImportError:
        # Fallback: try old location (pre-0.1.0)
        try:
            from langchain.agents import tool
        except ImportError:
            # Last resort: create a dummy decorator
            def tool(func):
                """Dummy tool decorator if LangChain tools aren't available"""
                return func

# PromptTemplate is not used in this code (using f-strings instead)
# from langchain.prompts import PromptTemplate

from langchain_google_genai import ChatGoogleGenerativeAI

# Database utilities
import sys
sys.path.insert(0, '/opt/airflow/src')
from db_utils import read_from_db, get_enhanced_table_context
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
AGENTS_DIR = '/agents'
LOGS_DIR = os.path.join(AGENTS_DIR, 'logs')
RESULTS_DIR = os.path.join(AGENTS_DIR, 'generated_results')
AGENT_LOGS_FILE = os.path.join(AGENTS_DIR, 'AGENT_LOGS.JSON')
METRICS_FILE = os.path.join(AGENTS_DIR, 'agent_metrics.json')

# Ensure directories exist
os.makedirs(LOGS_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)


def log_execution_time(func):
    """Decorator to log execution time of functions"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            execution_time = (time.time() - start_time) * 1000  # Convert to ms
            logger.info(f"{func.__name__} completed in {execution_time:.2f}ms")
            return result
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"{func.__name__} failed after {execution_time:.2f}ms: {e}")
            raise
    return wrapper


class NL2SQLAgent:
    """
    Natural Language to SQL Agent using Google Gemini and LangChain tools.
    """
    
    def __init__(self):
        """Initialize the NL2SQL agent with Gemini model."""
        self.api_key = os.getenv('GEMINI_API_KEY')
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY environment variable not set")
        
        # Configure Gemini
        genai.configure(api_key=self.api_key)
        
        # Initialize LangChain Gemini model
        self.llm = ChatGoogleGenerativeAI(
            model="gemini-2.5-flash",
            google_api_key=self.api_key,
            temperature=0.1,
            max_retries=3
        )
        
        self.table_name = 'stock_trades_integrated'
        self.max_retries = 3
        self.tools = self._create_tools()
        
        logger.info("NL2SQL Agent initialized with Gemini 2.5 Flash")
    
    def _create_tools(self) -> List:
        """Create LangChain tools for the agent."""
        
        @tool
        def read_user_query_file(file_path: str = "/agents/user_query.txt") -> str:
            """
            Read natural language query from user query file.
            
            Args:
                file_path: Path to the query file (default: /agents/user_query.txt)
            
            Returns:
                str: The user's natural language query
            """
            try:
                with open(file_path, 'r') as f:
                    query = f.read().strip()
                logger.info(f"Read query from {file_path}: {query[:100]}...")
                return query
            except Exception as e:
                logger.error(f"Error reading query file: {e}")
                return f"Error reading file: {str(e)}"
        
        @tool
        def execute_sql_query(sql: str, limit: int = 100) -> str:
            """
            Execute SQL query against the database and return formatted results.
            
            Args:
                sql: SQL query to execute
                limit: Maximum number of rows to return in preview (default: 100)
            
            Returns:
                str: Formatted results as markdown table with row count
            """
            try:
                # Execute query
                df = read_from_db(table_name=None, query=sql)
                
                total_rows = len(df)
                preview_rows = min(limit, total_rows)
                
                # Format as markdown table
                if preview_rows > 0:
                    preview_df = df.head(preview_rows)
                    markdown_table = preview_df.to_markdown(index=False)
                    result = f"Query returned {total_rows} rows. Showing first {preview_rows}:\n\n{markdown_table}"
                else:
                    result = "Query returned 0 rows."
                
                logger.info(f"Query executed successfully: {total_rows} rows")
                return result
                
            except Exception as e:
                error_msg = f"Error executing query: {str(e)}"
                logger.error(error_msg)
                return error_msg
        
        @tool
        def save_query_results_as_csv(sql: str, output_filename: Optional[str] = None) -> str:
            """
            Execute SQL query and save results to CSV file.
            
            Args:
                sql: SQL query to execute
                output_filename: Optional filename (auto-generated if not provided)
            
            Returns:
                str: Success message with file path
            """
            try:
                # Execute query
                df = read_from_db(table_name=None, query=sql)
                
                # Generate filename if not provided
                if output_filename is None:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    output_filename = f"query_results_{timestamp}.csv"
                
                # Ensure .csv extension
                if not output_filename.endswith('.csv'):
                    output_filename += '.csv'
                
                # Save to results directory
                output_path = os.path.join(RESULTS_DIR, output_filename)
                df.to_csv(output_path, index=False)
                
                logger.info(f"Saved {len(df)} rows to {output_path}")
                return f"Successfully saved {len(df)} rows to {output_path}"
                
            except Exception as e:
                error_msg = f"Error saving results: {str(e)}"
                logger.error(error_msg)
                return error_msg
        
        return [read_user_query_file, execute_sql_query, save_query_results_as_csv]
    
    @log_execution_time
    def validate_sql_safety(self, sql: str) -> Tuple[bool, str]:
        """
        Validate SQL query for safety and complexity constraints.
        
        Args:
            sql: SQL query to validate
        
        Returns:
            Tuple[bool, str]: (is_valid, error_message)
        """
        sql_upper = sql.upper()
        
        # Check for dangerous operations
        dangerous_keywords = [
            'DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER',
            'TRUNCATE', 'GRANT', 'REVOKE', 'CREATE', 'EXEC'
        ]
        
        for keyword in dangerous_keywords:
            if re.search(rf'\b{keyword}\b', sql_upper):
                return False, f"Dangerous operation detected: {keyword}"
        
        # Check for too many JOINs (max 3)
        join_count = len(re.findall(r'\bJOIN\b', sql_upper))
        if join_count > 3:
            return False, f"Too many JOINs: {join_count} (max 3 allowed)"
        
        # Ensure LIMIT clause exists or add it
        if 'LIMIT' not in sql_upper:
            sql = sql.rstrip(';') + ' LIMIT 1000;'
            logger.info("Added LIMIT 1000 to query")
        
        # Try to parse SQL
        try:
            parsed = sqlparse.parse(sql)
            if not parsed:
                return False, "Invalid SQL syntax"
        except Exception as e:
            return False, f"SQL parsing error: {str(e)}"
        
        return True, sql
    
    @log_execution_time
    def generate_sql(self, user_query: str, enhanced_context: dict) -> str:
        """
        Generate SQL query from natural language using Gemini.
        
        Args:
            user_query: Natural language query
            enhanced_context: Enhanced schema context with statistics
        
        Returns:
            str: Generated SQL query
        """
        # Format schema context
        schema_info = enhanced_context.get('schema', {})
        statistics_info = enhanced_context.get('statistics', {})
        
        # Build schema description
        schema_desc = f"Table: {self.table_name}\n\nColumns:\n"
        for col in schema_info.get('columns', []):
            schema_desc += f"- {col['name']} ({col['type']}): {col.get('description', '')}\n"
        
        # Build statistics context
        stats_desc = "\n\nColumn Statistics:\n"
        for col_name, stats in statistics_info.items():
            stats_desc += f"- {col_name}: {stats.get('unique_values', 'N/A')} unique values"
            if 'common_values' in stats:
                common = ', '.join(str(v) for v in stats['common_values'][:5])
                stats_desc += f", common: [{common}]"
            stats_desc += "\n"
        
        # Create prompt
        system_prompt = f"""You are an expert PostgreSQL query generator. Your task is to convert natural language questions into valid SQL queries.

{schema_desc}
{stats_desc}

RULES:
1. Generate ONLY valid SELECT queries
2. NEVER use DROP, DELETE, UPDATE, INSERT, ALTER, TRUNCATE, or other dangerous operations
3. Use proper JOIN syntax if needed
4. Include appropriate WHERE clauses based on the question
5. Use aggregate functions (SUM, AVG, COUNT) when appropriate
6. Add ORDER BY and LIMIT clauses when relevant
7. Return ONLY the SQL query without any explanation or markdown formatting

User Question: {user_query}

SQL Query:"""
        
        # Call Gemini with retries
        for attempt in range(self.max_retries):
            try:
                response = self.llm.invoke(system_prompt)
                sql = response.content.strip()
                
                # Clean up the response (remove markdown code blocks if present)
                sql = sql.replace('```sql', '').replace('```', '').strip()
                
                # Validate SQL
                is_valid, result = self.validate_sql_safety(sql)
                if is_valid:
                    logger.info(f"Generated SQL: {sql}")
                    return result
                else:
                    logger.warning(f"Invalid SQL attempt {attempt + 1}: {result}")
                    if attempt == self.max_retries - 1:
                        raise ValueError(f"Failed to generate valid SQL: {result}")
                
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed: {e}")
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
        
        raise ValueError("Failed to generate SQL after maximum retries")
    
    def update_metrics(self, success: bool, execution_time: float):
        """Update agent metrics file."""
        try:
            if os.path.exists(METRICS_FILE):
                with open(METRICS_FILE, 'r') as f:
                    metrics = json.load(f)
            else:
                metrics = {
                    "total_queries": 0,
                    "successful_queries": 0,
                    "failed_queries": 0,
                    "avg_execution_time": 0,
                    "cache_hit_rate": 0,
                    "last_updated": None
                }
            
            metrics['total_queries'] += 1
            if success:
                metrics['successful_queries'] += 1
            else:
                metrics['failed_queries'] += 1
            
            # Update average execution time
            total = metrics['total_queries']
            current_avg = metrics['avg_execution_time']
            metrics['avg_execution_time'] = ((current_avg * (total - 1)) + execution_time) / total
            
            metrics['last_updated'] = datetime.now().isoformat()
            
            with open(METRICS_FILE, 'w') as f:
                json.dump(metrics, f, indent=2)
            
        except Exception as e:
            logger.error(f"Error updating metrics: {e}")
    
    def save_detailed_log(self, log_data: dict):
        """Save detailed log to separate log file in logs directory."""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_filename = f"query_log_{timestamp}.json"
            log_path = os.path.join(LOGS_DIR, log_filename)
            
            with open(log_path, 'w') as f:
                json.dump(log_data, f, indent=2)
            
            logger.info(f"Detailed log saved to {log_path}")
            
        except Exception as e:
            logger.error(f"Error saving detailed log: {e}")
    
    def save_translation_log(self, user_query: str, sql_query: str):
        """Save simplified translation log to AGENT_LOGS.JSON."""
        try:
            if os.path.exists(AGENT_LOGS_FILE):
                with open(AGENT_LOGS_FILE, 'r') as f:
                    logs = json.load(f)
            else:
                logs = []
            
            log_entry = {
                "user_query": user_query,
                "agent_response": sql_query
            }
            
            logs.append(log_entry)
            
            with open(AGENT_LOGS_FILE, 'w') as f:
                json.dump(logs, f, indent=2)
            
            logger.info("Translation log updated")
            
        except Exception as e:
            logger.error(f"Error saving translation log: {e}")
    
    @log_execution_time
    def process_query(self, user_query: str, enhanced_context: dict) -> dict:
        """
        Complete pipeline: NL → SQL → Validation → Logging.
        
        Args:
            user_query: Natural language query
            enhanced_context: Enhanced schema context
        
        Returns:
            dict: Processing results with SQL and metadata
        """
        start_time = time.time()
        
        try:
            # Generate SQL
            sql = self.generate_sql(user_query, enhanced_context)
            
            execution_time = (time.time() - start_time) * 1000
            
            # Save simplified translation log
            self.save_translation_log(user_query, sql)
            
            # Create detailed log
            detailed_log = {
                "timestamp": datetime.now().isoformat(),
                "user_query": user_query,
                "generated_sql": sql,
                "model": "gemini-2.5-flash",
                "execution_time_ms": execution_time,
                "enhanced_context_used": enhanced_context.get('source') == 'database',
                "cache_hit": enhanced_context.get('source') == 'cache',
                "status": "success"
            }
            
            # Save detailed log to separate file
            self.save_detailed_log(detailed_log)
            
            # Update metrics
            self.update_metrics(success=True, execution_time=execution_time)
            
            return {
                "success": True,
                "sql": sql,
                "execution_time_ms": execution_time,
                "detailed_log_saved": True
            }
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            
            error_log = {
                "timestamp": datetime.now().isoformat(),
                "user_query": user_query,
                "error": str(e),
                "model": "gemini-2.5-flash",
                "execution_time_ms": execution_time,
                "status": "failed"
            }
            
            self.save_detailed_log(error_log)
            self.update_metrics(success=False, execution_time=execution_time)
            
            return {
                "success": False,
                "error": str(e),
                "execution_time_ms": execution_time
            }


# Tool functions for direct use in Airflow tasks
def read_query_file(file_path: str = "/agents/user_query.txt") -> str:
    """Read query from file."""
    with open(file_path, 'r') as f:
        return f.read().strip()


def execute_and_format_sql(sql: str, limit: int = 100) -> pd.DataFrame:
    """Execute SQL and return DataFrame."""
    return read_from_db(table_name=None, query=sql)


def save_results_to_csv(sql: str, output_filename: Optional[str] = None) -> str:
    """Save query results to CSV."""
    df = read_from_db(table_name=None, query=sql)
    
    if output_filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"query_results_{timestamp}.csv"
    
    if not output_filename.endswith('.csv'):
        output_filename += '.csv'
    
    output_path = os.path.join(RESULTS_DIR, output_filename)
    df.to_csv(output_path, index=False)
    
    return output_path
