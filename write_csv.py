# write_csv.py
import os
import csv
import threading
import re
import sys
from pathlib import Path
def get_base_dir():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

CSV_FIELDS = [
    "Name", "Rank", "Info",
    "EP1ID", "EP1Title", "EP1Info",
    "EP2ID", "EP2Title", "EP2Info",
    "EP3ID", "EP4ID", "EP4Title",
    "EP4Info", "EP5ID", "Awaken"
]
BASE_DIR = get_base_dir()
INDEX_PATH = os.path.join(BASE_DIR, "index.csv")
_csv_lock = threading.Lock()

def _load_existing():
    """既存 index.csv を Name+Rank をキーにして読み込む"""
    data = {}
    if not os.path.exists(INDEX_PATH):
        return data

    with open(INDEX_PATH, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = _make_key(row)
            data[key] = row
    return data

def normalize_for_csv(value):
    if not isinstance(value, str):
        return value
    # 改行・CRLF・タブを除去して1行化
    value = value.replace('\r', '').replace('\n', '')
    value = re.sub(r'\s+', ' ', value)
    return value.strip()

def _make_key(row):
    """重複判定キー（IDを使わない）"""
    return f"{row.get('Rank','')}::{row.get('Name','')}"


def write_rows(rows):
    """
    rows: list[dict]
    CSV_FIELDS に基づいて index.csv に書き込む
    """
    if not rows:
        return

    with _csv_lock:
        existing = _load_existing()

        for row in rows:
            key = _make_key(row)
            old = existing.get(key, {})

            merged = {}
            for field in CSV_FIELDS:
                val = row.get(field, "")
                val = normalize_for_csv(val)
                if val:
                    merged[field] = val
                else:
                    merged[field] = old.get(field, "")
            existing[key] = merged

        _save(existing)


def _save(data_dict):
    """CSV_FIELDS 順で保存"""
    rows = list(data_dict.values())

    # 安定ソート（Rank → Name）
    rows.sort(key=lambda r: (r.get("Rank", ""), r.get("Name", "")))

    with open(INDEX_PATH, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in CSV_FIELDS})
