# src/executors/oracle_executor.py
import os
from datetime import datetime
from src.connectors.oracle_connector import OracleConnector
from src.utils.io_utils import save_json_file

DEFAULT_OUT = os.getenv("OUTPUT_ORACLE", "output/oracle")

def execute_oracle_and_save(sql: str, out_dir: str = DEFAULT_OUT):
    oc = OracleConnector()
    conn = oc.get_connection()
    cur = conn.cursor()
    cur.execute(sql)
    cols = [c[0] for c in cur.description] if cur.description else []
    rows = cur.fetchall()
    results = []
    for r in rows:
        obj = {}
        for idx, col in enumerate(cols):
            val = r[idx]
            if hasattr(val, "isoformat"):
                val = val.isoformat()
            obj[col] = val
        results.append(obj)
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(out_dir, f"oracle_result_{ts}.json")
    save_json_file(results, filename)
    cur.close()
    conn.close()
    return filename, len(results)
