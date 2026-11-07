#!/usr/bin/env python3

import argparse
import concurrent.futures as cf
import configparser
import csv
import io
import json
import logging
import os
import re
import sys
import threading

import requests
import urllib3

import ksd_postprocess
import modifi_json
from download_portrait import download_portrait

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ============================================================
# English version URLs
# ============================================================

base_url = {
    "kamihime": {},
    "eidolon": {},
}

base_url["kamihime"]["info"] = (
    "https://gnkh-api-r.prod.nkh.dmmgames.com/v1/characters/"
)

base_url["kamihime"]["scenes"] = (
    "https://gnkh-api-r.prod.nkh.dmmgames.com/"
    "v1/gacha/harem_episodes/characters/"
)

base_url["eidolon"]["info"] = (
    "https://gnkh-api-r.prod.nkh.dmmgames.com/v1/summons/"
)

base_url["eidolon"]["scenes"] = (
    "https://gnkh-api-r.prod.nkh.dmmgames.com/"
    "v1/gacha/harem_episodes/summons/"
)

base_url["episode"] = (
    "https://gnkh-api-r.prod.nkh.dmmgames.com/v1/episodes/"
)

base_url["scene"] = (
    "https://gnkh-api-r.prod.nkh.dmmgames.com/v1/scenarios/"
)

static_base = (
    "https://gnkh-resource-r.prod.nkh.dmmgames.com/scenarios/"
)

ADV_TYPES = {
    "soul": {
        "suffix": "harem-job",
        "rank": "Soul Skin",
        "folder": "Soul Skin",
        "dir_name": "Soul_Skin({ep_id})",
    },
    "memorial": {
        "suffix": "harem-memorial",
        "rank": "Memorial",
        "folder": "Other",
        "dir_name": "(Memorial{ep_id})",
    },
    "burst": {
        "suffix": "harem-burst",
        "rank": "Burst",
        "folder": "Other",
        "dir_name": "(Burst{ep_id})",
    },
    "concierge": {
        "suffix": "harem-concierge",
        "rank": "Concierge",
        "folder": "Other",
        "dir_name": "(Concierge{ep_id})",
    },
}


# ============================================================
# Base directory / settings
# ============================================================

def get_base_dir():
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)

    return os.path.dirname(os.path.abspath(__file__))


BASE_DIR = get_base_dir()

SETTING_PATH = os.path.join(BASE_DIR, "setting.ini")

if not os.path.exists(SETTING_PATH):
    with open(SETTING_PATH, "w", encoding="utf-8") as f:
        f.write("[script]\nthreads = 8\n")

config = configparser.RawConfigParser()
config.read(SETTING_PATH)

thread_num = config.getint(
    "script",
    "threads",
    fallback=8
)


# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    filename=os.path.join(BASE_DIR, "error_nutaku.log"),
    filemode="a",
    level=logging.INFO,
    format="[%(levelname)s] %(asctime)s: %(message)s"
)


# ============================================================
# CSV
# ============================================================

CSV_FIELDS = [
    "Name", "Rank", "Info",
    "EP1ID", "EP1Title", "EP1Info",
    "EP2ID", "EP2Title", "EP2Info",
    "EP3ID", "EP4ID", "EP4Title",
    "EP4Info", "EP5ID", "Awaken"
]

PORTRAIT_AUDIT_FIELDS = [
    "character_id",
    "character_name",
    "rank",
    "portrait_type",
    "status",
    "portrait_status",
    "plain_text",
    "encrypted",
    "relative_path",
    "request_url",
    "http_status",
    "content_type",
    "response_size",
    "save_path",
    "error",
]


def normalize_for_csv(value):
    """
    Same normalization as write_csv.py.
    """
    if not isinstance(value, str):
        return value

    value = value.replace("\r", "").replace("\n", "")
    value = re.sub(r"\s+", " ", value)

    return value.strip()


def write_index_en(rows, path):
    """
    Append new records to index_en.csv using the same fields and
    normalization rules as the current write_csv.py.

    Unlike write_csv.write_rows(), this writes a separate
    English index file and never touches index.csv.
    """

    if not rows:
        logging.warning("No rows to write to %s", path)
        return

    existing_keys = set()
    has_header = False

    if os.path.exists(path):
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            has_header = reader.fieldnames is not None
            for existing_row in reader:
                existing_keys.add(
                    f"{existing_row.get('Rank', '')}::"
                    f"{existing_row.get('Name', '')}"
                )

    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=CSV_FIELDS)

    if not has_header:
        writer.writeheader()

    for row in rows:
        key = f"{row.get('Rank', '')}::{row.get('Name', '')}"
        if key in existing_keys:
            continue

        clean_row = {
            field: normalize_for_csv(row.get(field, ""))
            for field in CSV_FIELDS
        }
        writer.writerow(clean_row)
        existing_keys.add(key)

    if output.tell():
        with open(path, "a", encoding="utf-8", newline="") as f:
            f.write(output.getvalue())

    logging.info(
        "Nutaku_ver index written: %s",
        path
    )


def write_portrait_audit(rows, path):
    with open(
        path,
        "w",
        encoding="utf-8",
        newline=""
    ) as f:
        writer = csv.DictWriter(
            f,
            fieldnames=PORTRAIT_AUDIT_FIELDS,
            extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(rows)

    logging.info("Portrait audit written: %s", path)


# ============================================================
# latest.txt
# ============================================================

def load_latest(path):
    latest = {}

    if not os.path.exists(path):
        raise FileNotFoundError(
            f"latest.txt not found: {path}"
        )

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()

            if not line or line.startswith("#"):
                continue

            # latest.txt in the current downloader uses:
            #   kamihime_0: 100
            # but accept "=" as well for compatibility.
            if ":" in line:
                key, value = line.split(":", 1)
            elif "=" in line:
                key, value = line.split("=", 1)
            else:
                continue

            latest[key.strip()] = value.strip()

    return latest


def kamihime_bands_from_latest(latest_dict):
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


def generate_kamihime_ids(latest_dict):
    ids = []

    for band, max_count in kamihime_bands_from_latest(
        latest_dict
    ):
        base = int(round(band * 1000))

        for off in range(1, max_count + 1):
            ids.append(base + off)

    return ids


def eidolon_bands_from_latest(latest_dict):
    bands = []

    for key, value in latest_dict.items():

        if not key.startswith("eidolon_"):
            continue

        try:
            parts = key.replace("eidolon_", "").split("_")

            if len(parts) == 1:
                band = float(parts[0])
            elif len(parts) == 2:
                band = float(
                    f"{parts[0]}.{parts[1]}"
                )
            else:
                continue

            bands.append((band, int(value)))

        except ValueError:
            continue

    return bands


def generate_eidolon_ids(latest_dict):
    ids = []

    for band, max_count in eidolon_bands_from_latest(
        latest_dict
    ):
        base = int(round(band * 1000))

        for off in range(1, max_count + 1):
            ids.append(base + off)

    return ids


def generate_adv_episode_ids(latest, adv_type):
    """Generate additional-scenario IDs from latest.txt."""
    if adv_type not in latest:
        logging.warning("No latest entry found for adv type: %s", adv_type)
        return []

    try:
        count = int(latest[adv_type])
    except (TypeError, ValueError):
        logging.error("Invalid latest value for %s: %s", adv_type, latest[adv_type])
        return []

    if adv_type == "soul":
        return [8000 + i for i in range(1, count + 1)]

    return list(range(1, count + 1))


# ============================================================
# HTTP
# ============================================================

def create_session(session_token):
    headers = {
        "x-kh-session": session_token,
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 "
            "(KHTML, like Gecko) "
            "Chrome/81.0.4044.113 "
            "Safari/537.36"
        )
    }

    s = requests.Session()
    s.headers.update(headers)

    return s, headers


def get_json_result(s, url, headers, timeout=20):
    try:
        r = s.get(
            url,
            headers=headers,
            verify=False,
            timeout=timeout
        )

    except Exception as e:
        logging.error(
            "Request failed %s: %s",
            url,
            e
        )
        return None, f"Request failed: {e}"

    if r.status_code == 440:
        logging.error(
            "Session invalid or expired (440): %s",
            url
        )
        return None, "HTTP 440"

    if r.status_code != 200:
        logging.error(
            "HTTP %s: %s",
            r.status_code,
            url
        )
        return None, f"HTTP {r.status_code}"

    try:
        data = r.json()
    except Exception as e:
        logging.error(
            "JSON parse failed %s: %s",
            url,
            e
        )
        return None, f"JSON parse failed: {e}"

    if isinstance(data, dict) and "errors" in data:
        logging.error(
            "API returned errors for %s: %s",
            url,
            data.get("errors")
        )
        return None, f"API returned errors: {data.get('errors')}"

    return data, ""


def get_json(s, url, headers, timeout=20):
    return get_json_result(s, url, headers, timeout)[0]


def get_scene_info(
    s,
    scene_id,
    resource_directory,
    headers
):
    """
    Same scenario metadata lookup/fallback as the current
    Japanese downloader, except that the English API is used.
    """

    scene_url = (
        base_url["scene"]
        + str(scene_id)
    )

    data = get_json(
        s,
        scene_url,
        headers
    )

    if data is not None:
        return data

    resource_directory = resource_directory or ""

    resource_code = "/".join(
        resource_directory[-6:][i:i + 3]
        for i in range(
            0,
            len(resource_directory[-6:]),
            3
        )
    )

    return {
        "scenario_path": (
            f"{resource_code}/"
            f"{resource_directory}/"
            f"scenario.json"
        ),
        "resource_directory": resource_directory
    }


# ============================================================
# Filename handling
# ============================================================

def safe_name(name):
    """
    Same name conversion as the current downloader.
    """
    return (
        str(name)
        .replace("[", "(")
        .replace("]", ")")
    )


# ============================================================
# Common scene extraction
# ============================================================

def extract_scenes_from_episode(data):
    scenes = []

    try:
        chapter = data["chapters"][0]

        if (
            "scenarios" in chapter
            and chapter["scenarios"]
        ):
            sc = chapter["scenarios"][0]

            scenes.append({
                "id": sc["scenario_id"],
                "resource_directory":
                    sc.get("resource_directory")
            })

        if (
            "harem_scenes" in chapter
            and chapter["harem_scenes"]
        ):
            hs = chapter["harem_scenes"][0]

            scenes.append({
                "id": hs["harem_scene_id"],
                "resource_directory":
                    hs.get("resource_directory")
            })

    except Exception as e:
        logging.error(
            "Failed to parse episode chapter: %s",
            e
        )

    return scenes


# ============================================================
# English script download
# ============================================================

def download_english_script(
    s,
    headers,
    scene,
    save_dir
):
    """
    Download one English scenario.

    Save Nutaku scenarios in the same layout as the Japanese downloader.
    The metadata is saved as ``<id>.json`` and the payload as
    ``<id>_script.json`` (or ``<id>_script.ksd``).
    """

    file_name = scene["id"]

    scene_info = get_scene_info(
        s,
        file_name,
        scene.get("resource_directory"),
        headers
    )

    if not scene_info:
        return None

    scenario_path = scene_info.get(
        "scenario_path"
    )

    if not scenario_path:
        logging.warning(
            "No scenario_path for %s",
            file_name
        )
        return None

    url = static_base + scenario_path

    try:
        r = s.get(
            url,
            headers=headers,
            verify=False,
            timeout=20
        )

    except Exception as e:
        logging.error(
            "Static fetch failed %s: %s",
            url,
            e
        )
        return None

    if r.status_code != 200:
        logging.warning(
            "Scenario file missing (%s). Trying gameData.ksd fallback...",
            url
        )
        ksd_path = re.sub(r"[^/]+$", "gameData.ksd", scenario_path)
        ksd_url = static_base + ksd_path
        try:
            r = s.get(
                ksd_url,
                headers=headers,
                verify=False,
                timeout=20
            )
        except Exception as e:
            logging.error("Fallback gameData.ksd failed %s: %s", ksd_url, e)
            return None

        if r.status_code != 200:
            logging.error(
                "Both scenario and gameData.ksd missing: %s / %s",
                url,
                ksd_url
            )
            return None

        payload_name = f"{file_name}_script.ksd"
    elif scenario_path.endswith(".json"):
        payload_name = f"{file_name}_script.json"
    else:
        payload_name = f"{file_name}_script.ks"

    metadata_file = os.path.join(save_dir, f"{file_name}.json")
    payload_file = os.path.join(save_dir, payload_name)

    try:
        if not os.path.exists(metadata_file):
            with open(metadata_file, "w", encoding="utf-8") as f:
                json.dump(scene_info, f, ensure_ascii=False, indent=2)

        if not os.path.exists(payload_file):
            with open(payload_file, "wb") as f:
                f.write(r.content)
    except Exception as e:
        logging.error("Failed to save Nutaku scenario %s: %s", file_name, e)
        return None

    return scene_info


# ============================================================
# Kamihime
# ============================================================

def process_kamihime(
    kh_id,
    s,
    headers,
    save_root,
    portrait_audit_rows=None
):
    """
    Download the English Kamihime scenario set.

    Important:
      - English name is used for the directory.
      - A new English directory is created.
      - Japanese directory/files are never modified.
      - Portraits use the DMM resource URL with the Nutaku character ID.
    """

    TITLE_INFO_EP = {1, 2, 4}
    audit_rows = (
        portrait_audit_rows
        if portrait_audit_rows is not None
        else []
    )

    id_str = str(kh_id)

    # --------------------------------------------------------
    # Character info
    # --------------------------------------------------------

    url_info = (
        base_url["kamihime"]["info"]
        + id_str
    )

    info, info_error = get_json_result(
        s,
        url_info,
        headers
    )

    if not info:
        if not info_error:
            info_error = "empty character info response"
        audit_rows.append({
            "character_id": kh_id,
            "character_name": "",
            "rank": "",
            "portrait_type": "",
            "status": "character_info_failed",
            "portrait_status": "not_attempted",
            "plain_text": "",
            "encrypted": "",
            "relative_path": "",
            "request_url": "",
            "http_status": (
                info_error.split(" ", 2)[1]
                if info_error.startswith("HTTP ")
                else ""
            ),
            "content_type": "",
            "response_size": 0,
            "save_path": "",
            "error": info_error,
        })
        return None, audit_rows

    rarity = (
        info.get("rare")
        or info.get("rarity")
        or ""
    )

    raw_name = (
        info.get("name")
        or f"ID_{id_str}"
    )

    # IMPORTANT:
    # This is the English API name, so the directory is
    # also the English name.
    name = safe_name(raw_name)

    save_dir = os.path.join(
        save_root,
        f"{rarity} Kamihime",
        name
    )

    os.makedirs(
        save_dir,
        exist_ok=True
    )

    # Use the Nutaku portrait resource with the Nutaku character ID.
    audit_rows.extend(download_portrait(
        char_type="kamihime",
        char_id=kh_id,
        char_name=name,
        rank=rarity,
        service="Nutaku"
    ))

    csv_row = {
        "Name": name,
        "Rank": f"{rarity} Kamihime",
        "Info": info.get(
            "description",
            ""
        )
    }

    # --------------------------------------------------------
    # First episode
    # --------------------------------------------------------

    url_scenes = (
        base_url["kamihime"]["scenes"]
        + id_str
    )

    info_ep_1 = get_json(
        s,
        url_scenes,
        headers
    )

    if not info_ep_1:
        logging.error(
            "No scenes for Kamihime %s",
            id_str
        )
        return None, audit_rows

    try:
        ep_1_id = int(
            info_ep_1["episode_id"]
            .split("_")[0]
        )
    except Exception:
        logging.error(
            "Invalid episode_id for Kamihime %s",
            id_str
        )
        return None, audit_rows

    # --------------------------------------------------------
    # Same episode selection as Japanese downloader
    # --------------------------------------------------------

    eps = [ep_1_id - 1, ep_1_id]

    rare = info.get("rare", "")

    if rare == "SR":
        eps.append(ep_1_id + 1)

    elif rare == "SSR":
        if any(
            k in name
            for k in [
                "Awakened",
                "Reminiscence",
                "Pure Evil Intent",
                "Heart Ascension"
            ]
        ):
            pass

        elif "Heavenly Awakening" in name:
            eps = [ep_1_id]

        else:
            eps.append(ep_1_id + 1)

    # --------------------------------------------------------
    # Resolve episode scenes
    # --------------------------------------------------------

    scenes = []

    for ep in eps:

        url_ep = (
            base_url["episode"]
            + str(ep)
            + "_harem-character"
        )

        data = get_json(
            s,
            url_ep,
            headers
        )

        if not data:
            logging.error(
                "Episode detail missing %s",
                url_ep
            )
            continue

        scenes.extend(
            extract_scenes_from_episode(data)
        )

    if not scenes:
        logging.error(
            "No scenes resolved for Kamihime %s",
            kh_id
        )
        return None, audit_rows

    # --------------------------------------------------------
    # Download scripts / collect CSV data
    # --------------------------------------------------------

    ep_no = 1

    for scene in scenes:

        scene_info = download_english_script(
            s,
            headers,
            scene,
            save_dir
        )

        if not scene_info:
            continue

        file_name = scene["id"]

        csv_row[
            f"EP{ep_no}ID"
        ] = file_name

        if ep_no in TITLE_INFO_EP:
            csv_row[
                f"EP{ep_no}Title"
            ] = scene_info.get(
                "title",
                ""
            )

            csv_row[
                f"EP{ep_no}Info"
            ] = scene_info.get(
                "summary",
                ""
            )

        ep_no += 1

    return csv_row, audit_rows


# ============================================================
# Eidolon
# ============================================================

def process_eidolon(
    eid_id,
    s,
    headers,
    save_root
):
    """
    Download the English Eidolon scenario set.

    The English API name is used as the directory name.
    """

    TITLE_INFO_EP = {1, 2}

    id_str = str(eid_id)

    # --------------------------------------------------------
    # Eidolon info
    # --------------------------------------------------------

    url_info = (
        base_url["eidolon"]["info"]
        + id_str
    )

    info = get_json(
        s,
        url_info,
        headers
    )

    if not info:
        return None

    raw_name = (
        info.get("name")
        or f"ID_{id_str}"
    )

    name = safe_name(raw_name)

    save_dir = os.path.join(
        save_root,
        "Eidolon",
        name
    )

    os.makedirs(
        save_dir,
        exist_ok=True
    )

    # Use the Nutaku portrait resource with the Nutaku summon ID.
    download_portrait(
        char_type="eidolon",
        char_id=eid_id,
        char_name=name,
        service="Nutaku"
    )

    csv_row = {
        "Name": name,
        "Rank": "Eidolon",
        "Info": info.get(
            "description",
            ""
        )
    }

    # --------------------------------------------------------
    # First episode
    # --------------------------------------------------------

    url_scenes = (
        base_url["eidolon"]["scenes"]
        + id_str
    )

    info_ep_1 = get_json(
        s,
        url_scenes,
        headers
    )

    if not info_ep_1:
        logging.error(
            "No scenes for Eidolon %s",
            id_str
        )
        return None

    try:
        ep_1_id = int(
            info_ep_1["episode_id"]
            .split("_")[0]
        )
    except Exception:
        logging.error(
            "Bad episode_id for Eidolon %s",
            id_str
        )
        return None

    eps = [
        ep_1_id - 1,
        ep_1_id
    ]

    # --------------------------------------------------------
    # Resolve episode scenes
    # --------------------------------------------------------

    scenes = []

    for ep in eps:

        url_ep = (
            base_url["episode"]
            + str(ep)
            + "_harem-summon"
        )

        data = get_json(
            s,
            url_ep,
            headers
        )

        if not data:
            continue

        scenes.extend(
            extract_scenes_from_episode(data)
        )

    if not scenes:
        logging.error(
            "No scenes for Eidolon %s",
            eid_id
        )
        return None

    # --------------------------------------------------------
    # Download scripts / collect CSV data
    # --------------------------------------------------------

    ep_no = 1

    for scene in scenes:

        scene_info = download_english_script(
            s,
            headers,
            scene,
            save_dir
        )

        if not scene_info:
            continue

        file_name = scene["id"]

        csv_row[
            f"EP{ep_no}ID"
        ] = file_name

        if ep_no in TITLE_INFO_EP:
            csv_row[
                f"EP{ep_no}Title"
            ] = scene_info.get(
                "title",
                ""
            )

            csv_row[
                f"EP{ep_no}Info"
            ] = scene_info.get(
                "summary",
                ""
            )

        ep_no += 1

    return csv_row


# ============================================================
# Additional scenarios (Soul Skin / Memorial / Burst / Concierge)
# ============================================================

def process_adv_episode(
    ep_id,
    adv_type,
    s,
    headers,
    save_root
):
    config = ADV_TYPES[adv_type]
    dir_name = config["dir_name"].format(ep_id=ep_id)
    save_dir = os.path.join(
        save_root,
        config["folder"],
        dir_name
    )

    url = (
        base_url["episode"]
        + f"{ep_id}_{config['suffix']}"
    )
    data = get_json(s, url, headers)
    if not data:
        logging.error("%s episode missing: %s", config["rank"], url)
        return None

    scenes = extract_scenes_from_episode(data)
    if not scenes:
        logging.error("No scenes for %s episode %s", adv_type, ep_id)
        return None

    if os.path.exists(save_dir):
        logging.warning("Skip %s %s - already exists.", adv_type, ep_id)
        return None

    os.makedirs(save_dir, exist_ok=True)
    download_portrait(
        char_type=adv_type,
        char_id=ep_id,
        char_name=dir_name,
        service="Nutaku"
    )

    csv_row = {
        "Name": dir_name,
        "Rank": config["rank"],
    }
    ep_no = 1

    for scene in scenes:
        scene_info = download_english_script(
            s,
            headers,
            scene,
            save_dir
        )
        if not scene_info:
            continue

        csv_row[f"EP{ep_no}ID"] = scene["id"]
        if ep_no == 1:
            csv_row["EP1Title"] = scene_info.get("title", "")
            csv_row["EP1Info"] = scene_info.get("summary", "")
        ep_no += 1

    return csv_row


# ============================================================
# Main workflow
# ============================================================

def run_download_nutaku(
    session,
    save_root,
    latest_path=None,
    targets=None
):
    """
    English scenario downloader.

    Targets currently supported:
        kamihime
        eidolon
        soul
        memorial
        burst
        concierge

    The output is:
        <save_root>/.../<English Name>/*_script_en.ks
        <BASE_DIR>/index_en.csv
    """

    if targets is None:
        targets = [
            "kamihime",
            "eidolon",
            "soul",
            "memorial",
            "burst",
            "concierge",
        ]

    if latest_path is None:
        latest_path = os.path.join(
            BASE_DIR,
            "latest.txt"
        )

    latest = load_latest(
        latest_path
    )

    logging.info(
        "Loaded latest.txt: %d entries from %s",
        len(latest),
        latest_path
    )

    s, headers = create_session(
        session
    )

    csv_rows = []
    portrait_audit_rows = []

    # --------------------------------------------------------
    # Kamihime
    # --------------------------------------------------------

    if "kamihime" in targets:

        kh_ids = generate_kamihime_ids(
            latest
        )

        logging.info(
            "Nutaku Kamihime IDs: %d",
            len(kh_ids)
        )

        print(
            f"Kamihime IDs: {len(kh_ids)}"
        )

        with cf.ThreadPoolExecutor(
            max_workers=thread_num
        ) as executor:

            futures = {}
            for kh_id in kh_ids:
                task_audit_rows = []
                future = executor.submit(
                    process_kamihime,
                    kh_id,
                    s,
                    headers,
                    save_root,
                    task_audit_rows
                )
                futures[future] = (kh_id, task_audit_rows)

            for future in cf.as_completed(
                futures
            ):

                try:
                    row, audit_rows = future.result()

                    if isinstance(row, dict):
                        csv_rows.append(row)
                    portrait_audit_rows.extend(audit_rows)

                except Exception as error:
                    kh_id, task_audit_rows = futures[future]
                    portrait_audit_rows.extend(task_audit_rows)
                    logging.exception(
                        "Kamihime worker failed for ID %s",
                        kh_id
                    )
                    portrait_audit_rows.append({
                        "character_id": kh_id,
                        "character_name": "",
                        "rank": "",
                        "portrait_type": "",
                        "status": "worker_error",
                        "portrait_status": "unknown",
                        "plain_text": "",
                        "encrypted": "",
                        "relative_path": "",
                        "request_url": "",
                        "http_status": "",
                        "content_type": "",
                        "response_size": 0,
                        "save_path": "",
                        "error": str(error),
                    })

        audit_path = os.path.join(
            save_root,
            "portrait_audit.csv"
        )
        write_portrait_audit(
            portrait_audit_rows,
            audit_path
        )

    # --------------------------------------------------------
    # Eidolon
    # --------------------------------------------------------

    if "eidolon" in targets:

        eid_ids = generate_eidolon_ids(
            latest
        )

        logging.info(
            "Nutaku Eidolon IDs: %d",
            len(eid_ids)
        )

        print(
            f"Eidolon IDs: {len(eid_ids)}"
        )

        with cf.ThreadPoolExecutor(
            max_workers=thread_num
        ) as executor:

            futures = [
                executor.submit(
                    process_eidolon,
                    eid_id,
                    s,
                    headers,
                    save_root
                )
                for eid_id in eid_ids
            ]

            for future in cf.as_completed(
                futures
            ):

                try:
                    row = future.result()

                    if isinstance(row, dict):
                        csv_rows.append(row)

                except Exception:
                    logging.exception(
                        "Eidolon worker failed"
                    )

    for adv_type in ("soul", "memorial", "burst", "concierge"):
        if adv_type not in targets:
            continue

        episode_ids = generate_adv_episode_ids(latest, adv_type)
        logging.info(
            "Nutaku %s IDs: %d",
            adv_type,
            len(episode_ids)
        )

        with cf.ThreadPoolExecutor(max_workers=thread_num) as executor:
            futures = [
                executor.submit(
                    process_adv_episode,
                    ep_id,
                    adv_type,
                    s,
                    headers,
                    save_root
                )
                for ep_id in episode_ids
            ]

            for future in cf.as_completed(futures):
                try:
                    row = future.result()
                    if isinstance(row, dict):
                        csv_rows.append(row)
                except Exception:
                    logging.exception(
                        "%s worker failed",
                        adv_type
                    )

    # --------------------------------------------------------
    # English index
    # --------------------------------------------------------

    index_path = os.path.join(
        BASE_DIR,
        "index_en.csv"
    )

    write_index_en(
        csv_rows,
        index_path
    )

    logging.info("Start JSON modification under %s", save_root)
    modifi_json.process_root(save_root)

    if any(
        name.endswith(".ksd")
        for root, _, files in os.walk(save_root)
        for name in files
    ):
        assets_root = os.path.join(BASE_DIR, "assets")
        ksd_postprocess.process_root(save_root, assets_root)

    logging.info(
        "Nutaku scenario download completed."
    )

    return {
        "success": True,
        "rows": len(csv_rows),
        "index": index_path
    }


# ============================================================
# CLI
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description=(
            "Download Nutaku Kamihime/Eidolon "
            "scenario scripts and create index_en.csv."
        )
    )

    parser.add_argument(
        "session",
        help="x-kh-session"
    )

    parser.add_argument(
        "--save-root",
        required=True,
        help=(
            "Scenario data root. "
            "Nutaku folders will be created here."
        )
    )

    parser.add_argument(
        "--latest",
        default=os.path.join(
            BASE_DIR,
            "latest.txt"
        ),
        help="Path to latest.txt"
    )

    parser.add_argument(
        "--target",
        nargs="+",
        choices=[
            "kamihime",
            "eidolon"
        ],
        default=[
            "kamihime",
            "eidolon"
        ],
        help="Categories to download."
    )

    args = parser.parse_args()

    result = run_download_nutaku(
        session=args.session,
        save_root=args.save_root,
        latest_path=args.latest,
        targets=args.target
    )

    print(
        f"Completed: {result['rows']} rows"
    )

    print(
        f"index_en.csv: {result['index']}"
    )


if __name__ == "__main__":
    main()
