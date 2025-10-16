# src/schema_extractors/dwh_schema_extractor.py
import os
from typing import Dict, Tuple
from src.connectors.dwh_connector import DWHConnector
from src.utils.io_utils import save_json_file

"""
DWH schema extractor (SQL Server / INFORMATION_SCHEMA) - configurable and safe.

Environment variables supported:
- DWH_SCHEMA: comma-separated schema names to include (e.g. dbo, staging)
- DWH_TABLES: comma-separated table names to include (no schema)
- DWH_TABLE_PREFIX: table name prefix to include
- DWH_MAX_TABLES: integer maximum number of tables to extract (safe default applied)
- DWH_SAMPLE_COLUMNS: if set to 'true', we will only sample up to first N columns (not used here)
"""

DEFAULT_MAX_TABLES_SAFE = 20  # safety fallback to avoid extracting millions of tables

def _parse_csv_env(name: str):
    v = os.getenv(name)
    if not v:
        return None
    return [x.strip() for x in v.split(",") if x.strip()]

def extract_dwh_schema(output_path: str = "schema/dwh_schema.json") -> Dict[str, Dict]:
    dconn = DWHConnector()
    conn = dconn.get_connection()
    cur = conn.cursor()

    # read filters from env
    wanted_schemas = _parse_csv_env("DWH_SCHEMA")  # e.g. ["dbo","staging"]
    wanted_tables = _parse_csv_env("DWH_TABLES")   # e.g. ["MEMBER_DWH","ORDERS"]
    table_prefix = os.getenv("DWH_TABLE_PREFIX")
    max_tables = os.getenv("DWH_MAX_TABLES")
    try:
        max_tables = int(max_tables) if max_tables else None
    except Exception:
        max_tables = None

    # if no filters specified and no explicit max, apply safe default
    if not (wanted_schemas or wanted_tables or table_prefix or max_tables):
        max_tables = DEFAULT_MAX_TABLES_SAFE

    # Build WHERE clause pieces
    where_clauses = []
    params = {}

    if wanted_schemas:
        placeholders = ",".join(["?"] * len(wanted_schemas))
        where_clauses.append(f"TABLE_SCHEMA IN ({placeholders})")
        params.update({f"ps{i}": v for i, v in enumerate(wanted_schemas)})
        # pyodbc doesn't accept named params in the same way, we'll use positional binding later

    if wanted_tables:
        placeholders = ",".join(["?"] * len(wanted_tables))
        where_clauses.append(f"TABLE_NAME IN ({placeholders})")
        params.update({f"pt{i}": v for i, v in enumerate(wanted_tables)})

    if table_prefix:
        where_clauses.append("TABLE_NAME LIKE ?")
        params[f"pp0"] = f"{table_prefix}%"

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

    # Query INFORMATION_SCHEMA.COLUMNS with filtering. We'll fetch rows and then limit by distinct tables if needed.
    q = f"""
    SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE {where_sql}
    ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
    """
    # Build positional parameter list in the correct order
    param_values = []
    # order matters: wanted_schemas, wanted_tables, then prefix
    if wanted_schemas:
        param_values.extend(wanted_schemas)
    if wanted_tables:
        param_values.extend(wanted_tables)
    if table_prefix:
        param_values.append(f"{table_prefix}%")

    # Execute
    cur.execute(q, param_values if param_values else None)
    rows = cur.fetchall()

    # Build a mapping of (schema,table) -> list of columns. But if max_tables is set, only keep first N distinct tables.
    schema = {}
    distinct_table_keys = []
    for sch, tbl, col, dtype in rows:
        key = f"{sch.upper()}.{tbl.upper()}"
        if key not in schema:
            # if we already reached max_tables, skip creating new keys
            if max_tables and len(distinct_table_keys) >= max_tables:
                continue
            distinct_table_keys.append(key)
            schema[key] = {"columns": {}}
        # add column; if many duplicates, last wins but that's fine
        schema[key]["columns"][col.upper()] = dtype.upper()

    cur.close()
    conn.close()

    save_json_file(schema, output_path)
    print(f"[dwh_schema_extractor] saved {len(schema)} tables to {output_path}")
    return schema
