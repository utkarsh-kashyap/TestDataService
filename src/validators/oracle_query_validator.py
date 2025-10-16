# src/validators/oracle_query_validator.py
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

def validate_oracle_sql(sql: str, schema: dict):
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
        if t not in schema:
            issues.append(f"Unknown table referenced: {t}")
    alias_map = extract_alias_mapping(sql)
    qcols = extract_qualified_columns(sql)
    for qual, col in qcols:
        if qual in schema:
            if col not in schema[qual].get("columns", {}):
                issues.append(f"Unknown column {col} in table {qual}")
        elif qual in alias_map:
            real = alias_map[qual]
            if real not in schema or col not in schema[real].get("columns", {}):
                issues.append(f"Unknown column {col} in table {real} (alias {qual})")
    if issues:
        return False, "; ".join(issues)
    return True, "Validation passed"
