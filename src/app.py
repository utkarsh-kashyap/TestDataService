# src/app.py
import os
import argparse
import datetime
from dotenv import load_dotenv

from src.parsers.feature_parser import parse_examples
from src.utils.io_utils import load_json_file, append_history, save_json_file
from src.schema_extractors.oracle_schema_extractor import extract_oracle_schema
from src.schema_extractors.dwh_schema_extractor import extract_dwh_schema
from src.executors.oracle_executor import execute_oracle_and_save
from src.executors.dwh_executor import execute_dwh_and_save, dwh_execute_with_temp_table
from src.validators.oracle_query_validator import validate_oracle_sql
from src.validators.dwh_query_validator import validate_dwh_sql
from src.services.llm_client import call_llm

from src.connectors.oracle_connector import OracleConnector
from src.connectors.dwh_connector import DWHConnector

load_dotenv()

# Config paths
CONFIG_PATH = os.getenv("CONFIG_PATH", "config.json")
RULES_PATH = os.getenv("RULES_PATH", "rules.json")
ORACLE_SCHEMA_PATH = os.getenv("ORACLE_SCHEMA_PATH", "schema/oracle_schema.json")
DWH_SCHEMA_PATH = os.getenv("DWH_SCHEMA_PATH", "schema/dwh_schema.json")
HISTORY_PATH = os.getenv("HISTORY_PATH", "history/query_history.json")
ORACLE_OUT = os.getenv("OUTPUT_ORACLE", "output/oracle")
DWH_OUT = os.getenv("OUTPUT_DWH", "output/dwh")

# Adaptive batching defaults from env (with safe defaults)
DESIRED_COUNT = int(os.getenv("DESIRED_COUNT", "20"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
MAX_BATCHES = int(os.getenv("MAX_BATCHES", "10"))
EMAIL_PATTERN = os.getenv("EMAIL_PATTERN", "%@keyword.com%")
ORDER_BY = os.getenv("ORDER_BY_COLUMN", "NVL(LAST_UPDATED, CREATED_DATE) DESC")

# Okta/registration table info
OKTA_OWNER = os.getenv("OKTA_OWNER", "")
OKTA_TABLE = os.getenv("OKTA_TABLE", "OKTA_USERS")
OKTA_REGISTERED_FLAG_COL = os.getenv("OKTA_REGISTERED_FLAG_COL", "REGISTERED_FLAG")
OKTA_REGISTERED_FLAG_VALUE = os.getenv("OKTA_REGISTERED_FLAG_VALUE", "Y")

# owner/table substitution tokens
OWNER = os.getenv("SCHEMA_OWNER", "")
ORACLE_TABLE = os.getenv("ORACLE_TABLE", "MEMBER_MASTER")

# load config and rules
CONFIG = {}
try:
    CONFIG = load_json_file(CONFIG_PATH)
except Exception:
    CONFIG = {}

RULES = {}
try:
    RULES = load_json_file(RULES_PATH)
except Exception:
    RULES = {}

def render_template(sql_template: str, subs: dict):
    """
    Replace ${VAR} tokens and {placeholders} in the template.
    ${OWNER} style used for env tokens; {member_type} style for placeholders.
    """
    out = sql_template
    # first replace ${VAR}
    for k, v in subs.items():
        out = out.replace("${" + k + "}", str(v))
    # then format {} placeholders (safe)
    try:
        out = out.format(**subs)
    except Exception:
        # If formatting fails, leave as-is
        pass
    return out

def call_llm_batch_transform(single_member_sql: str, dialect: str, param_name: str, sample_values: list):
    """
    Ask LLM to convert a single-member SQL into a batched SQL that accepts multiple values.
    Returns SQL string or None on failure.
    """
    sample_txt = ", ".join([str(v) for v in sample_values[:50]])
    prompt = f"""You are a helpful SQL assistant.

The database dialect is: {dialect}.

I have a SQL template that takes a single parameter named '{param_name}'.
Here is the single-member SQL template (do NOT add explanation — return only ONE SQL statement):

{single_member_sql}

Produce a batched SQL that checks multiple values for {param_name}, using either an IN(...) list, array binding, or a safe temp-table approach appropriate for {dialect}. Use placeholder :{param_name}_list or {param_name}_list for bindings where appropriate.

Example sample values: {sample_txt}

Return exactly one SQL statement only (no commentary).
"""
    try:
        resp = call_llm(prompt, temperature=float(os.getenv("LLM_TEMPERATURE", "0.0")))
        return resp.strip()
    except Exception as e:
        print(f"[LLM transform] failed: {e}")
        return None

def fallback_make_in_clause(single_member_sql: str, param_placeholder: str, values: list):
    """
    Naive fallback: replace placeholder like '{user_no}' or :user_no with IN list.
    """
    sql = single_member_sql
    import re
    pattern1 = re.compile(r"([A-Za-z0-9_\.\"]+)\s*=\s*['\"]?\{?" + re.escape(param_placeholder) + r"\}['\"]?", re.IGNORECASE)
    m = pattern1.search(sql)
    if m:
        col = m.group(1)
        vals = []
        for v in values:
            if isinstance(v, (int, float)):
                vals.append(str(v))
            else:
                sv = str(v).replace("'", "''")
                vals.append(f"'{sv}'")
        in_list = ", ".join(vals)
        sql_new = pattern1.sub(f"{col} IN ({in_list})", sql)
        return sql_new
    vals = []
    for v in values:
        if isinstance(v, (int, float)):
            vals.append(str(v))
        else:
            sv = str(v).replace("'", "''")
            vals.append(f"'{sv}'")
    in_list = ", ".join(vals)
    return sql + f" WHERE {param_placeholder} IN ({in_list})"

def fetch_active_batch(conn: OracleConnector, active_template: str, member_type: str, email_pattern: str, offset: int, limit: int):
    """
    Use the single-member template to produce a paged query (by calling LLM to create a batch/offset version),
    else fallback to constructing OFFSET/FETCH version by substituting ORDER_BY and using OFFSET ... FETCH.
    """
    subs = {
        "OWNER": OWNER,
        "TABLE": ORACLE_TABLE,
        "member_type": member_type,
        "email_pattern": email_pattern,
        "ORDER_BY": ORDER_BY
    }
    single_sql = render_template(active_template, subs)

    # Try LLM to create a paginated/batched SQL for this offset/limit
    dialect = "Oracle"
    try:
        prompt = f"""You are an SQL assistant for Oracle. Convert the following SQL template into a paginated/batched SQL that returns rows between offset {offset} and limit {limit}. Use ORDER BY if required. Return only one SQL statement.

Template:
{single_sql}

Add paging: OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY.
"""
        paged_sql = call_llm(prompt)
        if paged_sql:
            paged_sql = paged_sql.strip().strip("`")
    except Exception:
        paged_sql = None

    # Fallback: try offset/fetch substitution
    if not paged_sql:
        base_sql = single_sql
        paged_sql = f"{base_sql} OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"

    conn_obj = conn.get_connection()
    cur = conn_obj.cursor()
    try:
        cur.execute(paged_sql)
        cols = [c[0] for c in cur.description] if cur.description else []
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        cur.close()
        conn_obj.close()
    return rows

def check_registered_batch(conn: OracleConnector, registered_template: str, user_nos: list):
    """
    Ask LLM to convert registered_template for a single user_no into a batch version, else fallback to IN(...) batch.
    """
    if not user_nos:
        return set()

    subs = {
        "OKTA_OWNER": OKTA_OWNER,
        "OKTA_TABLE": OKTA_TABLE,
        "OKTA_REGISTERED_FLAG_COL": OKTA_REGISTERED_FLAG_COL,
        "OKTA_REGISTERED_FLAG_VALUE": OKTA_REGISTERED_FLAG_VALUE
    }
    single_sql = render_template(registered_template, subs)

    try:
        batch_sql = call_llm_batch_transform(single_sql, "Oracle", "user_no", user_nos)
    except Exception:
        batch_sql = None

    if not batch_sql:
        batch_sql = fallback_make_in_clause(single_sql, "user_no", user_nos)

    conn_obj = conn.get_connection()
    cur = conn_obj.cursor()
    try:
        cur.execute(batch_sql)
        rows = cur.fetchall()
        registered = {r[0] for r in rows}
    finally:
        cur.close()
        conn_obj.close()
    return registered

def process_feature_examples(feature_path: str,
                             do_test_oracle=False,
                             do_test_dwh=False,
                             do_extract_oracle_schema=False,
                             do_extract_dwh_schema=False,
                             do_fetch_active=False,
                             fetch_member_type=None):
    """
    Main orchestrator. The boolean flags control early single-component execution.
    If any of the do_* flags are True, the function will run those components and return
    without running the full pipeline (unless no flags provided).
    """

    # If any single-component flags provided, run them and exit early (do not run full flow)
    # 1) test oracle connectivity
    if do_test_oracle:
        try:
            oc = OracleConnector()
            conn = oc.get_connection()
            conn.close()
            print("[test-oracle] Oracle connection successful.")
        except Exception as e:
            print(f"[test-oracle] Oracle connection FAILED: {e}")
        # if only testing oracle, continue to other flags or return if none other
        if not (do_test_dwh or do_extract_oracle_schema or do_extract_dwh_schema or do_fetch_active):
            return

    # 2) test dwh connectivity
    if do_test_dwh:
        try:
            dwhc = DWHConnector()
            conn = dwhc.get_connection()
            conn.close()
            print("[test-dwh] DWH connection successful.")
        except Exception as e:
            print(f"[test-dwh] DWH connection FAILED: {e}")
        if not (do_extract_oracle_schema or do_extract_dwh_schema or do_fetch_active):
            return

    # 3) extract oracle schema
    if do_extract_oracle_schema:
        try:
            extract_oracle_schema(ORACLE_SCHEMA_PATH)
            print("[extract-oracle-schema] completed.")
        except Exception as e:
            print(f"[extract-oracle-schema] FAILED: {e}")
        if not (do_extract_dwh_schema or do_fetch_active):
            return

    # 4) extract dwh schema
    if do_extract_dwh_schema:
        try:
            extract_dwh_schema(DWH_SCHEMA_PATH)
            print("[extract-dwh-schema] completed.")
        except Exception as e:
            print(f"[extract-dwh-schema] FAILED: {e}")
        if not do_fetch_active:
            return

    # 5) fetch active only
    if do_fetch_active:
        if not fetch_member_type:
            print("[fetch-active] requires --member-type argument.")
            return
        # Load config and templates
        if "queries" not in CONFIG:
            print("[fetch-active] config.json missing 'queries' section.")
            return
        active_template = CONFIG["queries"].get("active_members")
        if not active_template:
            print("[fetch-active] active_members template missing in config.json")
            return

        # We'll fetch only first batch (offset 0)
        oc = OracleConnector()
        rows = fetch_active_batch(oc, active_template, fetch_member_type, EMAIL_PATTERN, 0, BATCH_SIZE)
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        os.makedirs(ORACLE_OUT, exist_ok=True)
        out_file = os.path.join(ORACLE_OUT, f"oracle_active_{fetch_member_type}_{ts}.json")
        save_json_file(rows, out_file)
        print(f"[fetch-active] saved {len(rows)} rows to {out_file}")
        return

    # If none of the single-component flags were set, proceed with the full pipeline (existing behaviour)

    if not os.path.exists(feature_path):
        print(f"[app] feature file not found: {feature_path}")
        return

    examples = parse_examples(feature_path)
    if not examples:
        print("[app] no Examples table found in feature file.")
        return

    if not os.path.exists(ORACLE_SCHEMA_PATH):
        print("[app] extracting oracle schema...")
        extract_oracle_schema(ORACLE_SCHEMA_PATH)
    if not os.path.exists(DWH_SCHEMA_PATH):
        print("[app] extracting dwh schema...")
        extract_dwh_schema(DWH_SCHEMA_PATH)

    oracle_schema = load_json_file(ORACLE_SCHEMA_PATH)
    dwh_schema = load_json_file(DWH_SCHEMA_PATH)

    oc = OracleConnector()

    # Load templates
    if "queries" not in CONFIG:
        raise RuntimeError("config.json must include a 'queries' object with active_members and registered_members and dwh_query")

    active_template = CONFIG["queries"].get("active_members")
    registered_template = CONFIG["queries"].get("registered_members")
    dwh_template = CONFIG["queries"].get("dwh_query")

    if active_template is None or registered_template is None or dwh_template is None:
        raise RuntimeError("config.json queries must include active_members, registered_members and dwh_query")

    for idx, ex in enumerate(examples, start=1):
        ex_norm = {k.strip().lower(): v.strip() for k, v in ex.items()}
        mem_type = ex_norm.get("member_type") or ex_norm.get("member type")
        if not mem_type:
            print(f"[example {idx}] missing member_type; skipping")
            continue
        print(f"[example {idx}] member_type = {mem_type}")

        rule = RULES.get(mem_type) if RULES else None

        collected_active = []
        collected_registered = []
        registered_found = False

        # iterate batches
        for batch_idx in range(MAX_BATCHES):
            offset = batch_idx * BATCH_SIZE
            rows = fetch_active_batch(oc, active_template, mem_type, EMAIL_PATTERN, offset, BATCH_SIZE)
            if not rows:
                break
            collected_active.extend(rows)

            # extract user_nos to check registration in batch
            user_nos = [r.get("USER_NO") for r in rows if r.get("USER_NO") is not None]
            user_nos = list(dict.fromkeys([u for u in user_nos if u]))

            if user_nos:
                registered_set = check_registered_batch(oc, registered_template, user_nos)
                if registered_set:
                    for r in rows:
                        if r.get("USER_NO") in registered_set:
                            collected_registered.append(r)
                            if len(collected_registered) >= DESIRED_COUNT:
                                break
            if len(collected_registered) >= DESIRED_COUNT:
                registered_found = True
                break

        if registered_found and collected_registered:
            chosen = collected_registered[:DESIRED_COUNT]
        else:
            chosen = collected_active[:DESIRED_COUNT]
            registered_found = False

        # write chosen to oracle output JSON
        os.makedirs(ORACLE_OUT, exist_ok=True)
        oracle_out_file = os.path.join(ORACLE_OUT, f"oracle_candidates_example{idx}.json")
        save_json_file(chosen, oracle_out_file)

        # Build DWH query: ask LLM to transform single-member dwh_template into batch SQL
        member_ids = [m.get("MEMBER_ID") for m in chosen if m.get("MEMBER_ID") is not None]
        subs = {"OWNER": OWNER, "TABLE": ORACLE_TABLE}
        single_dwh_sql = render_template(dwh_template, subs)

        dwh_batch_sql = call_llm_batch_transform(single_dwh_sql, "SQLServer", "member_id", member_ids)
        dwh_out_file = None
        dwh_rows = 0
        try:
            if dwh_batch_sql and ("#members" in dwh_batch_sql or "CREATE TABLE" in dwh_batch_sql.upper()):
                dwh_conn = DWHConnector()
                results = dwh_execute_with_temp_table(dwh_conn, member_ids, dwh_batch_sql)
                os.makedirs(DWH_OUT, exist_ok=True)
                ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                dwh_out_file = os.path.join(DWH_OUT, f"dwh_result_example{idx}_{ts}.json")
                save_json_file(results, dwh_out_file)
                dwh_rows = len(results)
            else:
                batch_dwh_sql = fallback_make_in_clause(single_dwh_sql, "member_id", member_ids)
                ok, msg = validate_dwh_sql(batch_dwh_sql, dwh_schema)
                if not ok:
                    print(f"[example {idx}] DWH SQL validation failed: {msg}")
                else:
                    dwh_out_file, dwh_rows = execute_dwh_and_save(batch_dwh_sql, out_dir=DWH_OUT)
        except Exception as e:
            print(f"[example {idx}] DWH execution error: {e}")

        # append history
        entry = {
            "example_index": idx,
            "example": ex_norm,
            "registered_found": registered_found,
            "oracle_candidates_file": oracle_out_file,
            "chosen_count": len(chosen),
            "dwh_output_file": dwh_out_file,
            "dwh_rows": dwh_rows
        }
        append_history(HISTORY_PATH, entry)
        print(f"[example {idx}] done. registered_found={registered_found}, dwh_rows={dwh_rows}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--feature", default="features/user_login.feature", help="Path to .feature file")
    # New flags for running components individually
    parser.add_argument("--test-oracle", action="store_true", help="Test Oracle connection only and exit")
    parser.add_argument("--test-dwh", action="store_true", help="Test DWH connection only and exit")
    parser.add_argument("--extract-oracle-schema", action="store_true", help="Extract Oracle schema and exit")
    parser.add_argument("--extract-dwh-schema", action="store_true", help="Extract DWH schema and exit")
    parser.add_argument("--fetch-active", action="store_true", help="Fetch active members only (first batch) and exit")
    parser.add_argument("--member-type", type=str, help="Member type to use with --fetch-active")
    args = parser.parse_args()

    process_feature_examples(
        args.feature,
        do_test_oracle=args.test_oracle,
        do_test_dwh=args.test_dwh,
        do_extract_oracle_schema=args.extract_oracle_schema,
        do_extract_dwh_schema=args.extract_dwh_schema,
        do_fetch_active=args.fetch_active,
        fetch_member_type=args.member_type
    )

if __name__ == "__main__":
    main()
