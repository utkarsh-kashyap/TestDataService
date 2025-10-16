# src/validators/dwh_query_validator.py
import re
import sqlparse
from src.utils.sql_utils import extract_table_names, extract_qualified_columns, extract_alias_mapping

FORBIDDEN_KEYWORDS = ["DELETE", "DROP", "UPDATE", "INSERT", "ALTER", "TRUNCATE", "MERGE", "GRANT", "REVOKE"]

def contains_forbidden(sql: str):
    s = sql.upper()
    for kw in FORBIDDEN_KEYWORDS:
        if re.search(r'\b' + re.escape(kw) + r'\b', s):
            return True, kw
    return False, None

def is_select_query(sql: str):
    parsed = sqlparse.parse(sql)
    if not parsed:
        return False, "Could not parse SQL"
    stmt = parsed[0]
    for tok in stmt.tokens:
        if not tok.is_whitespace:
            first = tok
            break
    else:
        return False, "No tokens found"
    if getattr(first, "normalized", "").upper() in ("SELECT", "WITH"):
        return True, "SELECT/WITH"
    return False, f"Query must be SELECT or WITH; found: {first.value}"

def _schema_has_table(schema: dict, table_name: str):
    t = table_name.upper().split(".")[-1]
    for k in schema.keys():
        if k.upper().split(".")[-1] == t:
            return True
    return False

def validate_dwh_sql(sql: str, schema: dict):
    sql = sql.strip()
    if ";" in sql.rstrip().rstrip(";"):
        return False, "Multiple statements detected; only a single SELECT is allowed."
    forb, kw = contains_forbidden(sql)
    if forb:
        return False, f"Forbidden keyword found: {kw}"
    sel_ok, sel_msg = is_select_query(sql)
    if not sel_ok:
        return False, sel_msg
    issues = []
    tables = extract_table_names(sql)
    for t in tables:
        if not _schema_has_table(schema, t):
            issues.append(f"Unknown table referenced: {t}")
    alias_map = extract_alias_mapping(sql)
    qcols = extract_qualified_columns(sql)
    for qual, col in qcols:
        qual_last = qual.upper().split(".")[-1]
        matched_table_key = None
        for k in schema.keys():
            if k.upper().split(".")[-1] == qual_last:
                matched_table_key = k
                break
        if matched_table_key:
            if col not in schema[matched_table_key].get("columns", {}):
                issues.append(f"Unknown column {col} in table {matched_table_key}")
        elif qual in alias_map:
            real = alias_map[qual]
            found = False
            for k in schema.keys():
                if k.upper().split(".")[-1] == real:
                    if col not in schema[k].get("columns", {}):
                        issues.append(f"Unknown column {col} in table {k}")
                    found = True
                    break
            if not found:
                pass
        else:
            pass
    if issues:
        return False, "; ".join(issues)
    return True, "Validation passed"
