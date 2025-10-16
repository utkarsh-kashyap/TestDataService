# src/utils/sql_utils.py
import re
from typing import Set, Dict, Tuple

def strip_trailing_semicolon(sql: str) -> str:
    return sql.rstrip().rstrip(";")

def _strip_string_literals(sql: str) -> str:
    out = []
    i = 0
    in_quote = False
    L = len(sql)
    while i < L:
        ch = sql[i]
        if not in_quote:
            if ch == "'":
                in_quote = True
                out.append("''")
                i += 1
            else:
                out.append(ch)
                i += 1
        else:
            if ch == "'":
                if i + 1 < L and sql[i + 1] == "'":
                    i += 2
                else:
                    in_quote = False
                    out.append("''")
                    i += 1
            else:
                i += 1
    return "".join(out)

def extract_table_names(sql: str) -> Set[str]:
    sql_clean = re.sub(r'\s+', ' ', sql)
    pattern = re.compile(
        r'\b(?:FROM|JOIN|INTO)\s+(.+?)(?=\bWHERE\b|\bJOIN\b|\bON\b|\bGROUP\b|\bORDER\b|\bFETCH\b|\bLIMIT\b|;|$)',
        re.IGNORECASE
    )
    tables = set()
    for match in pattern.finditer(sql_clean):
        group = match.group(1).strip()
        if group.startswith("("):
            continue
        parts = re.split(r'\s*,\s*', group)
        for p in parts:
            token = p.split()[0].strip().strip('"').strip("'")
            if "." in token:
                token = token.split(".")[-1]
            token = re.sub(r'[^\w$#]', '', token)
            if token:
                tables.add(token.upper())
    return tables

def extract_alias_mapping(sql: str) -> Dict[str, str]:
    sql_clean = re.sub(r'\s+', ' ', sql)
    pattern = re.compile(
        r'\b(?:FROM|JOIN)\s+([A-Za-z0-9_$#".]+)\s+(?:AS\s+)?([A-Za-z0-9_$#"]+)\b',
        re.IGNORECASE
    )
    stop_words = {"WHERE", "ON", "JOIN", "GROUP", "ORDER", "FETCH", "LIMIT"}
    mapping = {}
    for m in pattern.finditer(sql_clean):
        raw_table = m.group(1)
        alias = m.group(2)
        if alias.upper() in stop_words:
            continue
        table = raw_table.strip('"').split(".")[-1].upper()
        alias = alias.strip('"').strip("'").upper()
        mapping[alias] = table
    return mapping

def extract_qualified_columns(sql: str) -> Set[Tuple[str, str]]:
    stripped = _strip_string_literals(sql)
    pairs = set()
    for match in re.finditer(r'\b([A-Za-z0-9_$#"]+)\.([A-Za-z0-9_$#"]+)\b', stripped):
        t = match.group(1).strip('"').upper()
        c = match.group(2).strip('"').upper()
        if '.' in t:
            t = t.split('.')[-1].upper()
        pairs.add((t, c))
    return pairs
