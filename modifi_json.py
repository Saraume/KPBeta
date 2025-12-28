import os
import json
import re
from pathlib import Path
import sys
def get_base_dir():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

BASE_DIR = get_base_dir()
TARGET_EXT = "_script.json"
ROOT_DIR = os.path.join(BASE_DIR, "scenariosForBeta")

# --- 末尾カンマ除去（000_modifi_json 相当） ---
def remove_trailing_commas(text: str) -> str:
    # } や ] の直前にあるカンマを削除
    text = re.sub(r",(\s*[}\]])", r"\1", text)
    return text

# --- scenario ラップ判定（mako 相当） ---
def is_wrapped_as_scenario(obj) -> bool:
    return (
        isinstance(obj, dict)
        and "scenario" in obj
        and len(obj) == 1
    )

# --- 末尾セミコロン除去（000_modifi_json 相当） ---
def remove_trailing_colons(text: str) -> str:
    # ファイル末尾の } または ] に続くセミコロンを削除
    text = re.sub(r"([}\]])\s*;\s*$", r"\1", text)
    return text


def process_json_file(file_path: str):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        # ① 末尾カンマ除去
        cleaned = remove_trailing_commas(content)

        # ② 末尾コロン除去（★追加）
        cleaned = remove_trailing_colons(cleaned)

        # ③ JSONとして読めるか確認
        parsed = json.loads(cleaned)

        # ④ scenario ラップ
        if not is_wrapped_as_scenario(parsed):
            wrapped = {"scenario": parsed}
        else:
            wrapped = parsed

        # ⑤ 最終検証
        json.dumps(wrapped, ensure_ascii=False)

        # ⑥ 保存
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(wrapped, f, ensure_ascii=False, indent=2)


        print(f"[OK] {file_path}")

    except Exception as e:
        print(f"[ERROR] {file_path}: {e}")

def process_root(root_dir: str):
    for root, _, files in os.walk(root_dir):
        for name in files:
            if name.endswith(TARGET_EXT):
                file_path = os.path.join(root, name)
                process_json_file(file_path)

def main():
    process_root(ROOT_DIR)
