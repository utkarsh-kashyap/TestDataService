# src/executors/dwh_executor.py
import os
from datetime import datetime
from src.connectors.dwh_connector import DWHConnector
from src.utils.io_utils import save_json_file

DEFAULT_OUT = os.getenv("OUTPUT_DWH", "output/dwh")

def execute_dwh_and_save(sql: str, out_dir: str = DEFAULT_OUT):
    dwh = DWHConnector()
    conn = dwh.get_connection()
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [c[0] for c in cur.description] if cur.description else []
    results = []
    for r in rows:
        obj = {}
        for idx, col in enumerate(cols):
            val = r[idx]
            try:
                if hasattr(val, "isoformat"):
                    val = val.isoformat()
            except Exception:
                pass
            obj[col] = val
        results.append(obj)
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(out_dir, f"dwh_result_{ts}.json")
    save_json_file(results, filename)
    cur.close()
    conn.close()
    return filename, len(results)

def dwh_execute_with_temp_table(dwh_conn: DWHConnector, member_ids: list, full_sql_using_temp_table: str):
    """
    full_sql_using_temp_table is expected to include creation & join to temp table.
    We'll open a connection, create temp table, bulk insert member ids, then execute and return results.
    This helper expects the SQL returned by LLM to reference a temp table named '#members' or similar.
    """
    if not member_ids:
        return []
    conn = dwh_conn.get_connection()
    cur = conn.cursor()
    try:
        # create temp table
        cur.execute("CREATE TABLE #members (member_id BIGINT);")
        # bulk insert
        rows_to_insert = [(int(mid),) for mid in member_ids]
        cur.fast_executemany = True
        cur.executemany("INSERT INTO #members (member_id) VALUES (?);", rows_to_insert)
        # run the provided SQL (which should reference #members)
        cur.execute(full_sql_using_temp_table)
        cols = [c[0] for c in cur.description] if cur.description else []
        results = [dict(zip(cols, r)) for r in cur.fetchall()]
        # clean up
        try:
            cur.execute("DROP TABLE #members;")
        except Exception:
            pass
        conn.commit()
        return results
    finally:
        cur.close()
        conn.close()
