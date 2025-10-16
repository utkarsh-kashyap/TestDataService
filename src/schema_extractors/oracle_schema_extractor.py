# src/schema_extractors/oracle_schema_extractor.py
import os
from dotenv import load_dotenv
from src.connectors.oracle_connector import OracleConnector
from src.utils.io_utils import save_json_file

load_dotenv()

def extract_oracle_schema(output_path: str = "schema/oracle_schema.json"):
    oc = OracleConnector()
    conn = oc.get_connection()
    cur = conn.cursor()

    owner = os.getenv("SCHEMA_OWNER")
    table_list_env = os.getenv("SCHEMA_TABLES")
    prefix = os.getenv("SCHEMA_TABLE_PREFIX")
    max_tables = int(os.getenv("SCHEMA_MAX_TABLES") or 0)

    if owner:
        cur.execute("SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = :owner ORDER BY TABLE_NAME", {"owner": owner.upper()})
        tables_raw = [r[0] for r in cur.fetchall()]
    else:
        cur.execute("SELECT TABLE_NAME FROM USER_TABLES ORDER BY TABLE_NAME")
        tables_raw = [r[0] for r in cur.fetchall()]

    if table_list_env:
        wanted = set([t.strip().upper() for t in table_list_env.strip().strip('"').strip("'").split(",") if t.strip()])
        tables = [t for t in tables_raw if t.upper() in wanted]
    elif prefix:
        pref = prefix.strip().upper()
        tables = [t for t in tables_raw if t.upper().startswith(pref)]
    else:
        tables = tables_raw

    if max_tables and len(tables) > max_tables:
        tables = tables[:max_tables]

    schema = {}
    for tbl in tables:
        if owner:
            q = """
            SELECT COLUMN_NAME, DATA_TYPE FROM ALL_TAB_COLUMNS
            WHERE OWNER = :owner AND TABLE_NAME = :tbl_name
            ORDER BY COLUMN_ID
            """
            cur.execute(q, {"owner": owner.upper(), "tbl_name": tbl.upper()})
        else:
            q = """
            SELECT COLUMN_NAME, DATA_TYPE FROM USER_TAB_COLUMNS
            WHERE TABLE_NAME = :tbl_name
            ORDER BY COLUMN_ID
            """
            cur.execute(q, {"tbl_name": tbl.upper()})
        cols = {row[0].upper(): row[1] for row in cur.fetchall()}
        schema[tbl.upper()] = {"columns": cols}

    cur.close()
    conn.close()

    save_json_file(schema, output_path)
    print(f"[oracle_schema_extractor] saved {len(schema)} tables to {output_path}")
    return schema
