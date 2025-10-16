# src/query_generators/dwh_query_generator.py
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
You are a SQL generator for SQL Server / T-SQL. Return ONE valid SQL SELECT only (no explanation).
- Use SQL Server syntax (DATEADD, TOP, OFFSET/FETCH).
- Do NOT use Oracle-specific functions.
- Use only tables/columns provided in schema.
- If provided, use the Oracle sample to restrict to relevant members.

=== SCHEMA (SCHEMA.TABLE: columns) ===
{schema_snippet}
=== END SCHEMA ===

=== EXAMPLES ===
{examples_snippet}
=== END EXAMPLES ===

{oracle_sample_block}

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

def generate_dwh_sql(user_input: str, dwh_schema: dict, oracle_sample: str = None) -> str:
    schema_snippet = _schema_to_lines(dwh_schema or {})
    examples_snippet = _examples_to_lines(CONFIG)
    oracle_sample_block = ""
    if oracle_sample:
        oracle_sample_block = "Oracle sample rows (use to restrict DWH query):\n" + oracle_sample + "\n=== END SAMPLE ==="
    prompt = PROMPT_TEMPLATE.format(schema_snippet=schema_snippet, examples_snippet=examples_snippet, oracle_sample_block=oracle_sample_block, user_input=user_input)
    resp = call_llm(prompt, temperature=float(os.getenv("LLM_TEMPERATURE", "0.0")))
    sql = strip_trailing_semicolon(resp).strip().strip("`")
    return sql
