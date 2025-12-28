#!/usr/bin/env python3

import requests
import os
import json
import sys
import urllib3
import logging
import configparser
import threading
import concurrent.futures as cf
import re
import write_csv
import modifi_json
from download_portrait import download_portrait

# base urls (original)
base_url = dict(soul={}, kamihime={}, eidolon={})
base_url['kamihime']['info'] = 'https://r.kamihimeproject.net/v1/characters/'
base_url['kamihime']['scenes'] = 'https://r.kamihimeproject.net/v1/gacha/harem_episodes/characters/'
base_url['eidolon']['info'] = 'https://r.kamihimeproject.net/v1/summons/'
base_url['eidolon']['scenes'] = 'https://r.kamihimeproject.net/v1/gacha/harem_episodes/summons/'
base_url['episode'] = 'https://r.kamihimeproject.net/v1/episodes/'
base_url['scene'] = 'https://r.kamihimeproject.net/v1/scenarios/'
static_base = 'https://static-r.kamihimeproject.net/scenarios/'

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ログ設定
logging.basicConfig(filename='0_error.log', filemode='a', level=logging.INFO,
                    format='[%(levelname)s] %(asctime)s: %(message)s')

def get_base_dir():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

BASE_DIR = get_base_dir()
SETTING_PATH = os.path.join(BASE_DIR, "setting.ini")

if not os.path.exists(SETTING_PATH):
    with open(SETTING_PATH, "w", encoding="utf-8") as f:
        f.write("[script]\nthreads = 8\n")

# iniファイルでスレッド数を定義
config = configparser.RawConfigParser()
config.read(SETTING_PATH)
thread_num = config.getint('script', 'threads', fallback=8)

ADV_TYPES = {
    "soul": {
        "suffix": "harem-job",
        "rank": "Soul Skin",
        "folder": "Soul Skin",
        "dir_name":"英霊スキン({ep_id})"
    },
    "memorial": {
        "suffix": "harem-memorial",
        "rank": "Memorial",
        "folder": "Other",
        "dir_name": "(メモリアル{ep_id})"
    },
    "burst": {
        "suffix": "harem-burst",
        "rank": "Burst",
        "folder": "Other",
        "dir_name": "(バースト{ep_id})"
    }
}

# -----------------------------
# band parsing helpers
# -----------------------------
def kamihime_default_bands(spec_list=None):
    # latestが読み込めなかったときにこの目次を使う（神姫）
    default = [(0, 100), (5, 600), (6, 300), (7, 200), (9, 50)]
    if spec_list is None:
        spec_list = default
    # return list of (x_float, y_int)
    return [(float(x), int(y)) for x,y in spec_list]

def parse_eidolon_bands_from_spec(spec_list=None):
    # latestが読み込めなかったときにこの目次を使う（幻獣）
    default = [(0,1),(0.011,35),(0.216,6),(2,10),(5,90),(6,300),(9.05,10),(9.2,20),(9.51,6)]
    if spec_list is None:
        spec_list = default
    # return list of (x_float, y_int)
    return [(float(x), int(y)) for x,y in spec_list]

def kamihime_bands_from_latest(latest_dict):
    """
    latest.txt の内容から kamihime 用 bands を生成する
    """
    bands = []

    for key, value in latest_dict.items():
        if not key.startswith("kamihime_"):
            continue

        try:
            x = float(key.replace("kamihime_", ""))
            y = int(value)
            bands.append((x, y))
        except ValueError:
            continue

    return sorted(bands)

def generate_kamihime_ids(latest_dict: dict):
    bands = kamihime_bands_from_latest(latest_dict)
    ids = []

    for band, max_count in bands:
        base = int(round(band * 1000))
        for off in range(1, max_count + 1):
            kh_id = base + off
            ids.append(("kamihime", off, kh_id))

    return ids

def eidolon_bands_from_latest(latest: dict) -> list[tuple[float, int]]:
    bands = []

    for key, value in latest.items():
        if not key.startswith("eidolon_"):
            continue

        try:
            parts = key.replace("eidolon_", "").split("_")
            if len(parts) == 1:
                band = float(parts[0])
            elif len(parts) == 2:
                band = float(f"{parts[0]}.{parts[1]}")
            else:
                continue

            max_count = int(value)
            bands.append((band, max_count))
        except ValueError:
            continue

    return bands

def generate_eidolon_ids(latest_dict: dict):
    bands = eidolon_bands_from_latest(latest_dict)
    ids = []

    for band, max_count in bands:
        base = int(round(band * 1000))
        for off in range(1, max_count + 1):
            eid = base + off
            ids.append(("eidolon", off, eid))

    return ids

def generate_adv_episode_ids(latest: dict, adv_type: str):
    """
    adv_type: 'soul' | 'memorial' | 'burst'
    return: list[int] episode_id list
    """

    if adv_type not in latest:
        logging.warning(f"No latest entry found for adv type: {adv_type}")
        return []

    try:
        count = int(latest[adv_type])
    except (TypeError, ValueError):
        logging.error(f"Invalid latest value for {adv_type}: {latest[adv_type]}")
        return []

    # Soul Skin: 8000番台
    if adv_type == "soul":
        base = 8000
        return [base + i for i in range(1, count + 1)]

    # Memorial / Burst: 1 から開始
    return list(range(1, count + 1))

# -----------------------------
# index collection (thread-safe)
# -----------------------------
index_lock = threading.Lock()
index_rows = []  # list of dicts for CSV writing

def add_index_row(row_dict):
    with index_lock:
        index_rows.append(row_dict)

def write_index_csv(path='index.csv'):
    # fields: id, category, name, rarity, save_path, other...
    if not index_rows:
        logging.error("No index rows to write.")
        return
    keys = ['category','id','name','rarity','save_path','note']
    with open(path, 'w', encoding='utf-8', newline='') as f:
        # simple CSV (utf-8); caller may convert to Shift_JIS if needed
        header = ",".join(keys) + "\n"
        f.write(header)
        with index_lock:
            for r in index_rows:
                vals = [str(r.get(k,"")) for k in keys]
                line = ",".join('"%s"'%v.replace('"','""') for v in vals) + "\n"
                f.write(line)
    logging.error(f"Wrote index to {path}")

# -----------------------------
# download_info helper (no file save unless requested)
# -----------------------------
def download_info_nosave(id_str, url, s, headers, save=False, save_folder=None):
    """
    GET the info JSON. If save True, will write to save_folder/<id>.json.
    Returns parsed JSON or None on error.
    """
    try:
        r = s.get(url, headers=headers, verify=False, timeout=15)
    except Exception as e:
        logging.error("Request failed %s : %s", url, e)
        return None
    if r.status_code == 440:
        logging.error("Token incorrect or expired (440). Exiting.")
        sys.exit(1)
    if r.status_code != 200:
        logging.error("Info not found or error (%s) for %s", r.status_code, url)
        return []
    try:
        info = r.json()
    except Exception as e:
        logging.error("JSON parse failed for %s : %s", url, e)
        return []
    if 'errors' in info:
        logging.error("API returned errors for %s : %s", url, info.get('errors'))
        return []
    if save and save_folder:
        os.makedirs(save_folder, exist_ok=True)
        file_path = os.path.join(save_folder, f"{id_str}.json")
        try:
            with open(file_path, 'w', encoding='utf-8') as of:
                json.dump(info, of, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.error("Failed to save info %s : %s", file_path, e)
    return info

# -----------------------------
# Core per-category workflow (reused original logic with small edits)
# -----------------------------
def process_kamihime_id(kh_id, s, headers, save_root):
    """
    kh_id: int
    - get info from /v1/characters/{kh_id}
    - get episodes (scenes) from /v1/gacha/harem_episodes/characters/{character_id}
    - download scenario files to SAVE_ROOT/{rarity} Kamihime/{name}/
    - add minimal index row to index_rows
    """

    TITLE_INFO_EP = {1, 2, 4}
    ep_no = 1

    id_str = str(kh_id)
    url_info = base_url['kamihime']['info'] + id_str
    info = download_info_nosave(id_str, url_info, s, headers, save=False)
    if not info:
        return []
    # filter by rarity/name
    rarity = info.get('rare') or info.get('rarity') or ""
    raw_name = info.get('name') or f"ID_{id_str}"

    # ファイル名用に安全化
    safe_name = (
        raw_name
        .replace('[', '(')
        .replace(']', ')')
    )

    name = safe_name
    
    csv_row = {
        "Name": name,
        "Rank": f"{rarity} Kamihime",
        "Info": info.get("description", "")
    }

    # --- スキップ処理（既存キャラフォルダがあればスキップ） ---
    save_dir = os.path.join(save_root, f"{rarity} Kamihime", name)
    if os.path.exists(save_dir):
        logging.warning(f"Skip {kh_id} — already exists.")
        return []
    os.makedirs(save_dir, exist_ok=True)

    # ★ ポートレートダウンロード
    download_portrait(
        char_type="kamihime",
        char_id=kh_id,
        char_name=name
    )

    # skip non SR/SSR? original code only targeted SR/SSR for third episode logic,
    # but original script attempted all characters. We keep downloading but skip non-targeted?
    # For safety, proceed but later filter as needed.
    # get scenes
    url_scenes = base_url['kamihime']['scenes'] + id_str
    r = s.get(url_scenes, headers=headers, verify=False)
    if r.status_code != 200:
        logging.error("No scenes for kh_id %s (%s)", id_str, r.status_code)
        return []
    try:
        info_ep_1 = r.json()
    except:
        logging.error("Invalid scenes JSON for %s", url_scenes)
        return []
    # derive episode ids similarly to original
    try:
        ep_1_id = int(info_ep_1['episode_id'].split('_')[0])
    except Exception:
        logging.error("Invalid episode_id structure for %s", url_scenes)
        return []
    eps = []
    if ep_1_id and isinstance(ep_1_id, int):
        name = info.get('name', '')
        rare = info.get('rare', '')

        # ベースは2話（例: R / 一部SSR）
        eps = [ep_1_id - 1, ep_1_id]

        # SRは必ず3話
        if rare == 'SR':
            eps.append(ep_1_id + 1)

        # SSRは条件分岐
        elif rare == 'SSR':
            if any(k in name for k in ['神化覚醒', '反心想', '純想悪', '心想昇華']):
                # これらは2話固定（既に2話リスト済み）
                pass
            elif '神想真化' in name:
                # 神想真化は1話だけ
                eps = [ep_1_id]
            else:
                # 通常SSRは3話
                eps.append(ep_1_id + 1)
    else:
        print(f"Warning: invalid ep_1_id for {info.get('name','Unknown')}")
    scenes = []
    for ep in eps:
        url_ep = base_url['episode'] + str(ep) + "_harem-character"
        r2 = s.get(url_ep, headers=headers, verify=False)
        if r2.status_code != 200:
            logging.error("episode detail missing %s", url_ep)
            continue
        try:
            data = r2.json()

        except Exception as e:
            logging.error("Invalid episode detail JSON %s: %s", url_ep, e)
            continue

        # extract scenarios/harem_scenes
        try:
            chapter = data['chapters'][0]
            if 'scenarios' in chapter and chapter['scenarios']:
                scenario = chapter['scenarios'][0]
                scenes.append({
                    "id": scenario['scenario_id'],
                    "resource_directory": scenario.get('resource_directory')
                })
            if 'harem_scenes' in chapter and chapter['harem_scenes']:
                hs = chapter['harem_scenes'][0]
                scenes.append({
                    "id": hs['harem_scene_id'],
                    "resource_directory": hs.get('resource_directory')
                })
        except Exception as e:
            logging.error("Failed parsing chapter for %s: %s", url_ep, e)

    if not scenes:
        logging.error("No scenes resolved for %s", kh_id)
        return []

    # for each scene, fetch scenario_info or construct path, then download static file
    saved_paths = []
    for scene in scenes:
        file_name = scene['id']
        # attempt to fetch scene meta via base_url['scene'] + file_name
        try:
            scene_url = base_url['scene'] + file_name
            r3 = s.get(scene_url, headers=headers, verify=False)
            if r3.status_code == 200:
                scene_info = r3.json()
            else:
                # fallback to construct scenario_path
                resource_directory = scene.get('resource_directory','')
                resource_code = '/'.join([resource_directory[-6:][i:i+3] for i in range(0, len(resource_directory[-6:]), 3)])
                scene_info = {"scenario_path": f"{resource_code}/{resource_directory}/scenario.json", "resource_directory": resource_directory}
        except Exception as e:
            logging.error("Scene meta fetch failed for %s: %s", file_name, e)
            continue

        # build static url and download .ks/.json
        scenario_path = scene_info.get('scenario_path')
        if not scenario_path:
            logging.info("No scenario_path for %s", file_name)
            continue
        ks_url = static_base + scenario_path
        try:
            rsc = s.get(ks_url, headers=headers, verify=False, timeout=20)
        except Exception as e:
            logging.error("Failed to get static %s : %s", ks_url, e)
            continue
        if rsc.status_code != 200:
            logging.error("Static file not found %s", ks_url)
            continue
        # determine extension and save
        is_json = scenario_path.endswith('.json')
        ext = 'json' if is_json else 'ks'
        # create save folder
        save_file = os.path.join(save_dir, f"{file_name}_script.{ext}")
        
        # CSV用のデータ格納処理
        # EPxID は必ず保存
        csv_row[f"EP{ep_no}ID"] = file_name

        # Title / Info は代表EPのみ
        if ep_no in TITLE_INFO_EP:
            csv_row[f"EP{ep_no}Title"] = scene_info.get("title", "")
            csv_row[f"EP{ep_no}Info"] = scene_info.get("summary", "")

        ep_no += 1

        # ★ jsonの元ファイルもここで保存する
        ep_json_path = os.path.join(save_dir, f"{file_name}.json")
        with open(ep_json_path, 'w', encoding='utf-8') as f:
            json.dump(scene_info, f, ensure_ascii=False, indent=2)

        try:
            with open(save_file, 'wb') as f:
                f.write(rsc.content)
            saved_paths.append(save_file)
        except Exception as e:
            logging.error("Failed to save static %s : %s", save_file, e)
            continue

    logging.info(f"{kh_id} - {name} was downloaded successfully")
    return csv_row


def process_eidolon_id(eid_id, s, headers, save_root):

    TITLE_INFO_EP = {1, 2}
    ep_no = 1
    
    id_str = str(eid_id)
    url_info = base_url['eidolon']['info'] + id_str
    info = download_info_nosave(id_str, url_info, s, headers, save=False)
    if not info:
        return []
    raw_name = info.get('name') or f"ID_{id_str}"

    # ファイル名用に安全化
    safe_name = (
        raw_name
        .replace('[', '(')
        .replace(']', ')')
    )

    name = safe_name

    csv_row = {
        "Name": name,
        "Rank": f"Eidolon",
        "Info": info.get("description", ""),
    }
    # --- スキップ処理（既存キャラフォルダがあればスキップ） ---
    save_dir = os.path.join(save_root, f"Eidolon", name)
    if os.path.exists(save_dir):
        logging.warning(f"Skip {eid_id} — already exists.")
        return []
    os.makedirs(save_dir, exist_ok=True)

    # ★ ポートレートダウンロード
    download_portrait(
        char_type="eidolon",
        char_id=eid_id,
        char_name=name
    )

    # these info dicts in original used 'summon_id'
    # get scenes via base_url['eidolon']['scenes'] + id
    url_scenes = base_url['eidolon']['scenes'] + id_str
    r = s.get(url_scenes, headers=headers, verify=False)
    if r.status_code != 200:
        logging.error("No eidolon scenes for %s", id_str)
        return []
    try:
        info_ep_1 = r.json()
    except:
        logging.error("Invalid eidolon scenes JSON %s", url_scenes)
        return []
    try:
        ep_1_id = int(info_ep_1['episode_id'].split('_')[0])
    except Exception:
        logging.error("Bad episode_id for eidolon %s", id_str)
        return []
    eps = [ep_1_id - 1, ep_1_id]
    scenes = []
    for ep in eps:
        url_ep = base_url['episode'] + str(ep) + "_harem-summon"
        r2 = s.get(url_ep, headers=headers, verify=False)
        if r2.status_code != 200:
            logging.error("eidolon episode missing %s", url_ep)
            continue
        try:
            data = r2.json()
        except:
            continue
        chapter = data['chapters'][0]
        if 'scenarios' in chapter and chapter['scenarios']:
            sc = chapter['scenarios'][0]
            scenes.append({"id": sc['scenario_id'], "resource_directory": sc.get('resource_directory')})
        if 'harem_scenes' in chapter and chapter['harem_scenes']:
            hs = chapter['harem_scenes'][0]
            scenes.append({"id": hs['harem_scene_id'], "resource_directory": hs.get('resource_directory')})
    if not scenes:
        logging.error("No scenes for eidolon %s", eid_id)
        return []
    
    saved_paths = []
    for scene in scenes:
        file_name = scene['id']
        try:
            r3 = s.get(base_url['scene'] + file_name, headers=headers, verify=False)
            if r3.status_code == 200:
                scene_info = r3.json()

            else:
                resource_directory = scene.get('resource_directory','')
                resource_code = '/'.join([resource_directory[-6:][i:i+3] for i in range(0, len(resource_directory[-6:]), 3)])
                scene_info = {"scenario_path": f"{resource_code}/{resource_directory}/scenario.json", "resource_directory": resource_directory}
        except Exception as e:
            logging.error("Scene meta fetch failed %s : %s", file_name, e)
            continue
        scenario_path = scene_info.get('scenario_path')
        if not scenario_path:
            continue
        ks_url = static_base + scenario_path
        try:
            rsc = s.get(ks_url, headers=headers, verify=False, timeout=20)
        except Exception as e:
            logging.error("Static fetch failed %s : %s", ks_url, e)
            continue
        if rsc.status_code != 200:
            continue
        is_json = scenario_path.endswith('.json')
        ext = 'json' if is_json else 'ks'
        save_file = os.path.join(save_dir, f"{file_name}_script.{ext}")

        # CSV用のデータ格納処理
        # EPxID は必ず保存
        csv_row[f"EP{ep_no}ID"] = file_name

        # Title / Info は代表EPのみ
        if ep_no in TITLE_INFO_EP:
            csv_row[f"EP{ep_no}Title"] = scene_info.get("title", "")
            csv_row[f"EP{ep_no}Info"] = scene_info.get("summary", "")

        ep_no += 1

        # ★ jsonの元ファイルもここで保存する
        ep_json_path = os.path.join(save_dir, f"{file_name}.json")
        with open(ep_json_path, 'w', encoding='utf-8') as f:
            json.dump(scene_info, f, ensure_ascii=False, indent=2)

        try:
            with open(save_file, 'wb') as f:
                f.write(rsc.content)
            saved_paths.append(save_file)
        except Exception as e:
            logging.error("Failed to save %s : %s", save_file, e)
            continue
    
    logging.info(f"{eid_id} - {name} was downloaded successfully")
    return csv_row

def process_adv_episode_id(ep_id: int, adv_type: str, s, headers, save_root):
    adv_conf = ADV_TYPES[adv_type]
    suffix = adv_conf["suffix"]
    rank = adv_conf["rank"]
    base_folder = adv_conf["folder"]
    dir_name = adv_conf["dir_name"].format(ep_id=ep_id)
    save_dir = os.path.join(
        save_root,
        base_folder,
        dir_name
    )
    csv_row = {
        "Name": dir_name,
        "Rank": base_folder,
        "Awaken": ""
    }

    scenes = []
    url_ep = base_url["episode"] + f"{ep_id}_{suffix}"
    r = s.get(url_ep, headers=headers, verify=False)
    if r.status_code != 200:
        logging.error(f"{rank} episode missing %s", url_ep)
        return []
    try:
        data = r.json()
    except Exception:
        return []
    chapter = data['chapters'][0]
    if 'scenarios' in chapter and chapter['scenarios']:
        sc = chapter['scenarios'][0]
        scenes.append({"id": sc['scenario_id'], "resource_directory": sc.get('resource_directory')})
    if 'harem_scenes' in chapter and chapter['harem_scenes']:
        hs = chapter['harem_scenes'][0]
        scenes.append({"id": hs['harem_scene_id'], "resource_directory": hs.get('resource_directory')})
    if not scenes:
        logging.error("No scenes for erisode %s", ep_id)
        return []
    
    # --- スキップ処理（既存フォルダがあればスキップ） ---
    if os.path.exists(save_dir):
        logging.warning(f"Skip {adv_type} {ep_id} — already exists.")
        return []
    os.makedirs(save_dir, exist_ok=True)

    # ★ポートレートダウンロード
    if adv_type == "soul":
        download_portrait(
            char_type="soul",
            char_id=ep_id,
            char_name=dir_name
        )
    if adv_type in ("memorial", "burst"):
        download_portrait(
            char_type=adv_type,
            char_id=ep_id,
            char_name=dir_name
        )

    # ===============================
    # adv 用 CSV 正規化ロジック
    # ===============================
    ep2_id = None
    ep2_title = ""
    ep2_info = ""
    ep3_id = None

    for scene in scenes:
        file_name = scene['id']
        try:
            r3 = s.get(base_url['scene'] + file_name, headers=headers, verify=False)
            if r3.status_code == 200:
                scene_info = r3.json()

            else:
                resource_directory = scene.get('resource_directory','')
                resource_code = '/'.join([resource_directory[-6:][i:i+3] for i in range(0, len(resource_directory[-6:]), 3)])
                scene_info = {"scenario_path": f"{resource_code}/{resource_directory}/scenario.json", "resource_directory": resource_directory}
        except Exception as e:
            logging.error("Scene meta fetch failed %s : %s", file_name, e)
            continue

        scenario_path = scene_info.get("scenario_path", "")
        if not scenario_path:
            continue
        is_ks = scenario_path.endswith(".ks")

        title = scene_info.get("title", "")
        summary = scene_info.get("summary", "")

        # --- EP2（代表） ---
        if title or summary:
            if is_ks:
                ep2_id = file_name
                ep2_title = title
                ep2_info = summary

            # --- EP3（重複） ---
            elif not is_ks:
                ep2_title = title
                ep2_info = summary
                ep3_id = file_name

        # --- 生 json 保存 ---
        ep_json_path = os.path.join(save_dir, f"{file_name}.json")
        with open(ep_json_path, "w", encoding="utf-8") as f:
            json.dump(scene_info, f, ensure_ascii=False, indent=2)

        # --- script 保存 ---
        try:
            rsc = s.get(static_base + scenario_path, headers=headers, verify=False, timeout=20)
            if rsc.status_code == 200:
                ext = "json" if scenario_path.endswith(".json") else "ks"
                save_file = os.path.join(save_dir, f"{file_name}_script.{ext}")
                with open(save_file, "wb") as f:
                    f.write(rsc.content)
        except Exception:
            pass

    # --- CSV 反映 ---
    if ep2_id:
        csv_row["EP2ID"] = ep2_id

    csv_row["EP2Title"] = ep2_title
    csv_row["EP2Info"] = ep2_info

    if ep3_id:
        csv_row["EP3ID"] = ep3_id
    
    logging.info(f"{adv_type}({ep_id}) was downloaded successfully")
    return csv_row

# -----------------------------
# utilities
# -----------------------------
#def sanitize_filename(name):
    # remove forbidden chars for filenames on Windows etc.
#    return re.sub(r'[\\/:"*?<>|]+', '_', name)

# -----------------------------
# main orchestration
# -----------------------------
def run_download_json(
    session: str,
    target: list[str],
    latest_dict: dict,
    save_root: str,
    modify_json: bool):

    result = {
        "success": True,
        "message": "",
        "errors": [],
        "counts": {
            "kamihime": 0,
            "eidolon": 0,
            "soul": 0,
            "memorial": 0,
            "burst": 0,
        }
    }

    headers = {
        'x-kh-session': session,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36'
    }
    s = requests.Session()
    s.headers.update(headers)

    # 保存先ディレクトリ
    os.makedirs(save_root, exist_ok=True)

    csv_rows = []

    # Kamihime
    if 'kamihime' in target:
        logging.info("Generating Kamihime ID list from latest.txt ...")
        kh_list = generate_kamihime_ids(latest_dict)     # latestから探索するIDを抽出
        kh_ids = [tpl[2] for tpl in kh_list]
        logging.info(f"Kamihime ids to try: {len(kh_ids)}")
        with cf.ThreadPoolExecutor(max_workers=thread_num) as exc:
            futures = [exc.submit(process_kamihime_id, kh, s, headers, save_root) for kh in kh_ids]
            for fut in cf.as_completed(futures):
                try:
                    res = fut.result()
                    if isinstance(res, dict):
                        csv_rows.append(res)
                        result["counts"]["kamihime"] += 1
                except Exception as e:
                    logging.error("Error in kamihime worker: %s", e)
                    result["success"] = False
                    result["errors"].append(str(e))

    # Eidolon
    if 'eidolon' in target:
        logging.info("Generating Eidolon ID list from latest.txt ...")
        eid_list = generate_eidolon_ids(latest_dict)
        eid_ids = [tpl[2] for tpl in eid_list]
        logging.info(f"Eidolon ids to try: {len(eid_ids)}")
        with cf.ThreadPoolExecutor(max_workers=thread_num) as exc:
            futures = [exc.submit(process_eidolon_id, eid, s, headers, save_root) for eid in eid_ids]
            for fut in cf.as_completed(futures):
                try:
                    res = fut.result()
                    if isinstance(res, dict):
                        csv_rows.append(res)
                        result["counts"]["eidolon"] += 1

                except Exception as e:
                    logging.error("Error in eidolon worker: %s", e)
                    result["success"] = False
                    result["errors"].append(str(e))

    # Soul Skin
    if 'soul' in target:
        ep_ids = generate_adv_episode_ids(latest_dict, 'soul')
        logging.info(f"Soul Skin episodes to try: {len(ep_ids)}")

        with cf.ThreadPoolExecutor(max_workers=thread_num) as exc:
            futures = [
                exc.submit(process_adv_episode_id, ep_id, 'soul', s, headers, save_root)
                for ep_id in ep_ids
            ]
            for fut in cf.as_completed(futures):
                try:
                    res = fut.result()
                    if isinstance(res, dict):
                        csv_rows.append(res)
                        result["counts"]["soul"] += 1

                except Exception as e:
                    logging.error("Error in soul worker: %s", e)
                    result["success"] = False
                    result["errors"].append(str(e))

    # Memorial
    if 'memorial' in target:
        ep_ids = generate_adv_episode_ids(latest_dict, 'memorial')
        logging.info(f"memorial episodes to try: {len(ep_ids)}")

        with cf.ThreadPoolExecutor(max_workers=thread_num) as exc:
            futures = [
                exc.submit(process_adv_episode_id, ep_id, 'memorial', s, headers ,save_root)
                for ep_id in ep_ids
            ]
            for fut in cf.as_completed(futures):
                try:
                    res = fut.result()
                    if isinstance(res, dict):
                        csv_rows.append(res)
                        result["counts"]["memorial"] += 1

                except Exception as e:
                    logging.error("Error in memorial worker: %s", e)
                    result["success"] = False
                    result["errors"].append(str(e))

    # Burst
    if 'burst' in target:
        ep_ids = generate_adv_episode_ids(latest_dict, 'burst')
        logging.info(f"burst episodes to try: {len(ep_ids)}")

        with cf.ThreadPoolExecutor(max_workers=thread_num) as exc:
            futures = [
                exc.submit(process_adv_episode_id, ep_id, 'burst', s, headers, save_root)
                for ep_id in ep_ids
            ]
            for fut in cf.as_completed(futures):
                try:
                    res = fut.result()
                    if isinstance(res, dict):
                        csv_rows.append(res)
                        result["counts"]["burst"] += 1
                        
                except Exception as e:
                    logging.error("Error in burst worker: %s", e)
                    result["success"] = False
                    result["errors"].append(str(e))

    write_csv.write_rows(csv_rows)
    logging.info("CSV written via write_csv.py")

    if modify_json:
        logging.info("Start JSON modification under %s", save_root)
        try:
            modifi_json.process_root(save_root)
        except Exception:
            logging.exception("JSON modification failed")
    
    # message 組み立て
    result["message"] = (
        f"Completed.\n"
        f"Kamihime: {result['counts']['kamihime']}\n"
        f"Eidolon: {result['counts']['eidolon']}\n"
        f"Soul: {result['counts']['soul']}\n"
        f"Memorial: {result['counts']['memorial']}\n"
        f"Burst: {result['counts']['burst']}"
    )

    return result

