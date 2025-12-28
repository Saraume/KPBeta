import os
import urllib.request
from Crypto.Cipher import Blowfish
from Crypto.Util.Padding import pad
import sys
from pathlib import Path

def get_base_dir():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

BASE_DIR = get_base_dir()

# =============================
# 設定
# =============================

# ここは実環境に合わせて固定
ICON_DIR = os.path.join(BASE_DIR, "portrait")
ILLUST_DIR = os.path.join(BASE_DIR, "portrait_full")

os.makedirs(ICON_DIR, exist_ok=True)
os.makedirs(ILLUST_DIR, exist_ok=True)

# =============================
# タイプ別ルール（設計の核）
# =============================

PORTRAIT_RULES = {
    "kamihime": {
        "icon": "corecard_chara_",
        "illust": "illustzoom_chara_",
        "id_offset": 0,
    },
    "eidolon": {
        "icon": "corecard_summon_",
        "illust": "illustzoom_summon_",
        "id_offset": 0,
    },
    "soul": {
        "icon": "corecard_job_",
        "illust": "illustzoom_job_",
        "id_offset": 0,
    },
    "memorial": {
        "icon": "corecard_item_",
        "illust": None,
        "id_offset": 30000,
    },
    "burst": {
        "icon": "corecard_item_",
        "illust": None,
        "id_offset": 32000,
    },
}

# =============================
# 暗号化処理（既存仕様そのまま）
# =============================

def kamihime_encrypt(data: str) -> str:
    key = b"bLoWfIsH"
    cipher = Blowfish.new(key, Blowfish.MODE_ECB)
    padded = pad(data.encode("utf-8"), 8)
    return cipher.encrypt(padded).hex()

def get_path(t: str, p: str) -> str:
    r = t.rfind(".")
    e = t[:r] if r != -1 else t
    part1 = e[-6:-3]
    part2 = e[-3:]
    ext = ".png" if ("illust" in p or "harem" in p) else ".jpg"
    return f"{part1}/{part2}/{t}{ext}"

def build_url(type_str: str, x: str) -> str:
    if "corecard_item_" in type_str:
        data = type_str + x
    elif "questimg_harem" in type_str:
        data = type_str + x + "_1"
    else:
        data = type_str + x + "_0"

    final = kamihime_encrypt(data)
    path = get_path(final, type_str)
    return "https://static-r.kamihimeproject.net/resources/pc/normal/" + path

# =============================
# ダウンロード処理
# =============================

def download_image(url: str, save_path: str) -> bool:
    try:
        urllib.request.urlretrieve(url, save_path)
        return True
    except Exception:
        return False

# =============================
# 外部API（0_download_json から呼ぶ）
# =============================

def download_portrait(char_type: str, char_id: int, char_name: str):
    rule = PORTRAIT_RULES.get(char_type)
    if not rule:
        return

    real_id = char_id + rule["id_offset"]

    # --- アイコン ---
    icon_url = build_url(rule["icon"], str(real_id))
    icon_path = os.path.join(ICON_DIR, f"{char_name}.jpg")
    download_image(icon_url, icon_path)

    # --- 立ち絵（必要なタイプのみ） ---
    if rule["illust"]:
        illust_url = build_url(rule["illust"], str(real_id))
        illust_path = os.path.join(ILLUST_DIR, f"{char_name}.png")
        download_image(illust_url, illust_path)
