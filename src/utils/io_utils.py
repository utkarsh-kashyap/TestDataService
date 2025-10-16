# src/utils/io_utils.py
import json
import os
from typing import Any

def load_json_file(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)

def save_json_file(obj: Any, path: str):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, indent=2, default=str)

def append_history(history_path: str, entry: dict):
    os.makedirs(os.path.dirname(history_path) or ".", exist_ok=True)
    hist = []
    if os.path.exists(history_path):
        try:
            hist = load_json_file(history_path)
        except Exception:
            hist = []
    hist.append(entry)
    save_json_file(hist, history_path)
