# src/parsers/feature_parser.py
import re
from typing import List, Dict

def parse_examples(feature_path: str) -> List[Dict[str, str]]:
    txt = open(feature_path, "r", encoding="utf-8").read()
    m = re.search(r"Examples:\s*\n((?:\s*\|.*\n)+)", txt, re.IGNORECASE)
    if not m:
        return []
    block = m.group(1).strip()
    lines = [l.strip() for l in block.splitlines() if l.strip().startswith("|")]
    headers = [h.strip().lower() for h in lines[0].strip("|").split("|")]
    rows = []
    for line in lines[1:]:
        vals = [v.strip() for v in line.strip("|").split("|")]
        if len(vals) < len(headers):
            vals += [""] * (len(headers) - len(vals))
        row = {headers[i]: vals[i] for i in range(len(headers))}
        rows.append(row)
    return rows
