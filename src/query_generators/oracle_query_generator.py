# src/query_generators/oracle_query_generator.py
import os
import json
from dotenv import load_dotenv
from src.services.llm_client import call_llm
from src.utils.sql_utils import strip_trailing_semicolon

load_dotenv()
CONFIG_PATH = os.getenv("CONFIG_PATH", "config.json")
try:
    CONFIG = json.load(open(CONFIG_PATH, "r", encoding="utf-8"))
except Exception:
    CONFIG = {}

PROMPT_TEMPLATE = """
You are a SQL generator for Oracle. Return ONE valid SQL SELECT only (no explanation).
- Oracle syntax only (ADD_MONTHS, ROWNUM, TO_CHAR, etc).
- Do NOT use DML/DDL.
- Use only tables/columns provided in schema.

=== SCHEMA ===
{schema_snippet}
=== END SCHEMA ===

=== EXAMPLES ===
{examples_snippet}
=== END EXAMPLES ===

User request:
\"\"\"{user_input}\"\"\"

Return only the SQL:
"""

def _schema_to_lines(schema: dict):
    lines = []
    for table, info in (schema or {}).items():
        cols = ", ".join(info.get("columns", {}).keys())
        lines.append(f"{table}: {cols}")
    return "\n".join(lines)

def _examples_to_lines(config: dict):
    eq = config.get("example_queries", {}) or {}
    return "\n".join([f"{k}: {v}" for k, v in eq.items()])

def generate_oracle_sql(user_input: str, oracle_schema: dict) -> str:
    schema_snippet = _schema_to_lines(oracle_schema or {})
    examples_snippet = _examples_to_lines(CONFIG)
    prompt = PROMPT_TEMPLATE.format(schema_snippet=schema_snippet, examples_snippet=examples_snippet, user_input=user_input)
    resp = call_llm(prompt, temperature=float(os.getenv("LLM_TEMPERATURE", "0.0")))
    sql = strip_trailing_semicolon(resp).strip().strip("`")
    return sql
