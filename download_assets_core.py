import requests
import json
import os
import sys
import re
import urllib3
import logging
import configparser
import threading
import concurrent.futures as cf
from collections import Counter
from urllib.parse import urlsplit, urlunsplit
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(
    filename='1_error.log',
    filemode='w',
    level='INFO',
    format='[%(levelname)s] %(asctime)s: %(message)s'
)
from ksd_postprocess import fix_ogg_files

DEFAULT_ASSET_BASE_URL = 'https://static-r.kamihimeproject.net/scenarios/'
NUTAKU_ASSET_BASE_URL = 'https://gnkh-resource-r.prod.nkh.dmmgames.com/scenarios/'


def build_asset_url(asset_base_url, asset_path):
    return f"{asset_base_url.rstrip('/')}/{asset_path}"


def safe_url_for_log(url):
    parts = urlsplit(url)
    hostname = parts.hostname or ''
    if parts.port:
        hostname = f"{hostname}:{parts.port}"
    return urlunsplit((parts.scheme, hostname, parts.path, '', ''))


def get_base_dir():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

BASE_DIR = get_base_dir()
SETTING_PATH = os.path.join(BASE_DIR, "setting.ini")
if not os.path.exists(SETTING_PATH):
    with open(SETTING_PATH, "w", encoding="utf-8") as f:
        f.write("[script]\nthreads = 8\n")
# Number of threads to donwload assets
config = configparser.RawConfigParser()
config.read(SETTING_PATH)
thread_num = config.getint('script', 'threads')

# Set request timeout (seconds)
req_timeout = 120

ignore_links = []
retry_links = []
retry_num = 3

ignore_file = os.path.join(BASE_DIR, "ignore.txt")

if os.path.exists(ignore_file):
    with open(ignore_file, 'r', encoding="utf-8") as f:
        ignore_links = f.read().splitlines()
    ignore_links_len = len(ignore_links)
else:
    ignore_links_len = 0

links = []
asset_folder = os.path.join(BASE_DIR, 'assets')

headers = {}
headers['user-agent'] = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'

_ASSET_SESSION = None

def get_asset_session():
    global _ASSET_SESSION
    if _ASSET_SESSION is None:
        s = requests.Session()
        s.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
        })
        _ASSET_SESSION = s
    return _ASSET_SESSION


############################################################################
# MAIN FUNCTIONS
############################################################################
def download_script(script_path, url, scenario_id, metadata_filename):
    logged_url = safe_url_for_log(url)
    try:
        response = get_asset_session().get(
            url,
            headers=headers,
            verify=False,
            timeout=req_timeout
        )
    except requests.exceptions.RequestException:
        logging.exception(
            "Scenario script request failed: scenario_id=%s metadata=%s "
            "URL=%s destination=%s",
            scenario_id,
            metadata_filename,
            logged_url,
            script_path
        )
        return False

    content_type = response.headers.get('Content-Type', '')
    received_bytes = len(response.content)
    if response.status_code != 200:
        logging.error(
            "Scenario script download failed: scenario_id=%s metadata=%s "
            "URL=%s status=%s content_type=%s bytes=%d destination=%s",
            scenario_id,
            metadata_filename,
            logged_url,
            response.status_code,
            content_type,
            received_bytes,
            script_path
        )
        return False

    try:
        content = response.text
        if script_path.endswith('json'):
            content = re.sub(
                '(\\],(?=\\s*\\}))',
                ']',
                re.sub('(\\},(?=\\s*\\]))', '}', content)
            )
            content = re.sub('(?!\\}),(?=\\s*\\})', '', content)
            content = re.sub('\\];', ']', content)
        with open(script_path, 'w', encoding='utf-8') as file:
            file.write(content)
            file.flush()
    except OSError:
        logging.exception(
            "Scenario script save failed: scenario_id=%s metadata=%s "
            "URL=%s status=%s content_type=%s bytes=%d destination=%s",
            scenario_id,
            metadata_filename,
            logged_url,
            response.status_code,
            content_type,
            received_bytes,
            script_path
        )
        return False

    logging.info(
        "Scenario script saved: scenario_id=%s metadata=%s URL=%s status=%s "
        "content_type=%s bytes=%d destination=%s save=success",
        scenario_id,
        metadata_filename,
        logged_url,
        response.status_code,
        content_type,
        received_bytes,
        script_path
    )
    return True


def download_asset(
    link,
    resource_directory,
    downloaded_ogg_files=None,
    asset_kind="unknown",
    scenario_id="unknown",
    metadata_filename="unknown"
):
    link = link.replace(' ', '')
    logged_url = safe_url_for_log(link)
    folder = os.path.join(asset_folder, resource_directory)
    filename = urlsplit(link).path.rsplit('/', 1)[-1]
    dst = os.path.join(folder, filename).replace('_pc_h', '')

    logging.info(
        "Asset URL generated: scenario_id=%s metadata=%s kind=%s URL=%s "
        "destination=%s",
        scenario_id,
        metadata_filename,
        asset_kind,
        logged_url,
        dst
    )

    if os.path.exists(dst):
        logging.info(
            "Asset already exists: scenario_id=%s metadata=%s kind=%s "
            "URL=%s destination=%s save=skipped",
            scenario_id,
            metadata_filename,
            asset_kind,
            logged_url,
            dst
        )
        return "skipped"

    if link in ignore_links:
        logging.warning(
            "Asset ignored: scenario_id=%s metadata=%s kind=%s URL=%s "
            "destination=%s save=failed reason=previously_not_found",
            scenario_id,
            metadata_filename,
            asset_kind,
            logged_url,
            dst
        )
        return "failed"

    try:
        os.makedirs(folder, exist_ok=True)
    except OSError:
        logging.exception(
            "Asset destination directory creation failed: scenario_id=%s "
            "metadata=%s kind=%s URL=%s destination=%s save=failed",
            scenario_id,
            metadata_filename,
            asset_kind,
            logged_url,
            dst
        )
        return "failed"

    try:
        response = get_asset_session().get(
            link,
            headers=headers,
            verify=False,
            timeout=req_timeout
        )
    except requests.exceptions.RequestException:
        logging.exception(
            "Asset request failed: scenario_id=%s metadata=%s kind=%s "
            "URL=%s status=unavailable content_type=unavailable bytes=0 "
            "destination=%s save=failed",
            scenario_id,
            metadata_filename,
            asset_kind,
            logged_url,
            dst
        )
        return "failed"

    content_type = response.headers.get('Content-Type', '')
    if response.status_code != 200:
        received_bytes = len(response.content)
        logging.error(
            "Asset request failed: scenario_id=%s metadata=%s kind=%s "
            "URL=%s status=%s content_type=%s bytes=%d destination=%s "
            "save=failed",
            scenario_id,
            metadata_filename,
            asset_kind,
            logged_url,
            response.status_code,
            content_type,
            received_bytes,
            dst
        )
        if response.status_code == 404:
            ignore_links.append(link)
        return "failed"

    received_bytes = 0
    first_chunk = True
    try:
        with open(dst, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if not chunk:
                    continue
                received_bytes += len(chunk)
                if first_chunk:
                    first_chunk = False
                    if chunk.lstrip().lower().startswith((b'<html', b'<!doctype html')):
                        logging.error(
                            "Asset response is HTML: scenario_id=%s metadata=%s "
                            "kind=%s URL=%s status=%s content_type=%s bytes=%d "
                            "destination=%s save=failed",
                            scenario_id,
                            metadata_filename,
                            asset_kind,
                            logged_url,
                            response.status_code,
                            content_type,
                            received_bytes,
                            dst
                        )
                        file.close()
                        os.remove(dst)
                        return "failed"
                file.write(chunk)
    except (OSError, requests.exceptions.RequestException):
        logging.exception(
            "Asset save failed: scenario_id=%s metadata=%s kind=%s URL=%s "
            "status=%s content_type=%s bytes=%d destination=%s save=failed",
            scenario_id,
            metadata_filename,
            asset_kind,
            logged_url,
            response.status_code,
            content_type,
            received_bytes,
            dst
        )
        if os.path.exists(dst):
            os.remove(dst)
        return "failed"

    if received_bytes == 0:
        logging.error(
            "Asset response is empty: scenario_id=%s metadata=%s kind=%s "
            "URL=%s status=%s content_type=%s bytes=0 destination=%s "
            "save=failed",
            scenario_id,
            metadata_filename,
            asset_kind,
            logged_url,
            response.status_code,
            content_type,
            dst
        )
        os.remove(dst)
        return "failed"

    if downloaded_ogg_files is not None and dst.lower().endswith(".ogg"):
        downloaded_ogg_files.append(dst)

    logging.info(
        "Asset saved: scenario_id=%s metadata=%s kind=%s URL=%s status=%s "
        "content_type=%s bytes=%d destination=%s save=success",
        scenario_id,
        metadata_filename,
        asset_kind,
        logged_url,
        response.status_code,
        content_type,
        received_bytes,
        dst
    )
    return "downloaded"

def download_assets(
    links,
    resource_directory='',
    downloaded_ogg_files=None,
    scenario_id="unknown",
    metadata_filename="unknown"
):
    asset_counts = Counter(asset_kind for asset_kind, _ in links)
    logging.info(
        "Assets extracted: scenario_id=%s metadata=%s counts=%s",
        scenario_id,
        metadata_filename,
        dict(asset_counts)
    )
    if not links:
        return {"downloaded": 0, "failed": 0, "skipped": 0}

    totals = Counter()
    with cf.ThreadPoolExecutor(max_workers=thread_num) as executor:
        futures = {
            executor.submit(
                download_asset,
                link,
                resource_directory,
                downloaded_ogg_files,
                asset_kind,
                scenario_id,
                metadata_filename
            ): (asset_kind, link)
            for asset_kind, link in links
        }

        for future in cf.as_completed(futures):
            asset_kind, link = futures[future]
            try:
                result = future.result()
            except Exception:
                logging.exception(
                    "Asset worker raised: scenario_id=%s metadata=%s kind=%s "
                    "URL=%s destination=unknown save=failed",
                    scenario_id,
                    metadata_filename,
                    asset_kind,
                    safe_url_for_log(link)
                )
                totals["failed"] += 1
                continue
            if result not in ("downloaded", "failed", "skipped"):
                logging.error(
                    "Asset worker returned an invalid result: scenario_id=%s "
                    "metadata=%s kind=%s URL=%s result=%r",
                    scenario_id,
                    metadata_filename,
                    asset_kind,
                    safe_url_for_log(link),
                    result
                )
                totals["failed"] += 1
                continue
            totals[result] += 1

    return {
        "downloaded": totals["downloaded"],
        "failed": totals["failed"],
        "skipped": totals["skipped"]
    }


def download_scenario_assets(
    character,
    scenario_type,
    filename,
    data,
    data_directory,
    downloaded_ogg_files,
    asset_base_url=DEFAULT_ASSET_BASE_URL,
    scenario_id=None,
    metadata_filename=None
):
    scenario_id = scenario_id or os.path.splitext(filename)[0]
    metadata_filename = metadata_filename or filename
    script_file = '%s_script.ks' % filename.replace('.json', '').replace('.ks', '')
    script_path = os.path.join(data_directory, scenario_type, character, script_file)

    # Download script file if not exists
    if not os.path.exists(script_path):
        print ("Downloading script file...")
        if not download_script(
            script_path,
            build_asset_url(asset_base_url, data['scenario_path']),
            scenario_id,
            metadata_filename
        ):
            print ("Failed to download script for %s" % filename)
            logging.error(
                "Could not read KS scenario: scenario_id=%s metadata=%s "
                "format=KS script=%s",
                scenario_id,
                metadata_filename,
                script_path
            )
            return {"downloaded": 0, "failed": 1, "skipped": 0}

    try:
        with open(script_path, encoding='utf-8') as file:
            script = file.read()
    except (OSError, UnicodeError):
        logging.exception(
            "Could not read KS scenario: scenario_id=%s metadata=%s "
            "format=KS script=%s",
            scenario_id,
            metadata_filename,
            script_path
        )
        return {"downloaded": 0, "failed": 1, "skipped": 0}

    logging.info(
        "Scenario format detected: scenario_id=%s metadata=%s format=KS "
        "script=%s",
        scenario_id,
        metadata_filename,
        script_path
    )

    links = []
    for match in re.finditer(r'\[chara_face.*storage="(.*)"', script):
        links.append((
            "character_image",
            build_asset_url(asset_base_url, 'fgimage/' + match.group(1))
        ))

    for match in re.finditer(r'\[playbgm.*storage="(.*)"', script):
        links.append((
            "bgm",
            build_asset_url(asset_base_url, 'bgm/' + match.group(1))
        ))

    for match in re.finditer(r'\[bg.*storage="(.*)"', script):
        bg_path = re.sub(
            r"(.*)(-.*)",
            r"\1_pc_h\2",
            match.group(1)
        )
        links.append((
            "background",
            build_asset_url(asset_base_url, 'bgimage/' + bg_path)
        ))

    totals = Counter(download_assets(
        links,
        '',
        downloaded_ogg_files,
        scenario_id,
        metadata_filename
    ))

    links = []
    for match in re.finditer(r'\[playse.*storage="(.*)"', script):
        sound_path = '/'.join(
            data['scenario_path'].split('/')[:3]
        ) + '/sound/' + match.group(1)
        links.append((
            "sound_effect",
            build_asset_url(asset_base_url, sound_path)
        ))

    totals.update(download_assets(
        links,
        data["resource_directory"],
        downloaded_ogg_files,
        scenario_id,
        metadata_filename
    ))
    return dict(totals)


def download_hscene_assets(
    character,
    scenario_type,
    filename,
    data,
    data_directory,
    downloaded_ogg_files,
    asset_base_url=DEFAULT_ASSET_BASE_URL,
    scenario_id=None,
    metadata_filename=None
):
    scenario_id = scenario_id or os.path.splitext(filename)[0]
    metadata_filename = metadata_filename or filename
    script_file = f"{filename.replace('.json', '').replace('.ks', '')}_script.json"
    script_path = os.path.join(data_directory, scenario_type, character, script_file)

    # スクリプトファイルがなければダウンロード
    if not os.path.exists(script_path):
        print("Downloading script file...")
        if not download_script(
            script_path,
            build_asset_url(asset_base_url, data['scenario_path']),
            scenario_id,
            metadata_filename
        ):
            print(f"Failed to download script for {filename}")
            logging.error(
                "Could not read JSON scenario: scenario_id=%s metadata=%s "
                "script=%s",
                scenario_id,
                metadata_filename,
                script_path
            )
            return {"downloaded": 0, "failed": 1, "skipped": 0}

    try:
        with open(script_path, encoding='utf-8') as file:
            script = json.load(file)
    except (OSError, UnicodeError, json.JSONDecodeError):
        logging.exception(
            "Could not parse JSON scenario: scenario_id=%s metadata=%s "
            "script=%s",
            scenario_id,
            metadata_filename,
            script_path
        )
        return {"downloaded": 0, "failed": 1, "skipped": 0}

    if (
        isinstance(script, dict)
        and {"version", "root", "assets"}.issubset(script)
    ):
        logging.error(
            "Scenario format detected but unsupported: scenario_id=%s "
            "metadata=%s format=Helix script=%s",
            scenario_id,
            metadata_filename,
            script_path
        )
        return {"downloaded": 0, "failed": 1, "skipped": 0}

    if not isinstance(script, dict) or not isinstance(script.get("scenario"), list):
        logging.error(
            "Scenario format is unrecognized: scenario_id=%s metadata=%s "
            "script=%s",
            scenario_id,
            metadata_filename,
            script_path
        )
        return {"downloaded": 0, "failed": 1, "skipped": 0}

    logging.info(
        "Scenario format detected: scenario_id=%s metadata=%s "
        "format=legacy_JSON script=%s",
        scenario_id,
        metadata_filename,
        script_path
    )

    links = []
    resource_path = data['scenario_path'][:data['scenario_path'].rfind('/')]
    scenes = script.get("scenario", [])

    for section in scenes:
        # `bgm` の処理
        if 'bgm' in section:
            links.append((
                "bgm",
                build_asset_url(
                    asset_base_url,
                    f"{resource_path}/{section['bgm']}"
                )
            ))

        # `film` の処理（新形式対応）
        if 'film' in section:
            if isinstance(section['film'], list):
                # リストの場合は .jpg で終わる要素のみ抽出
                film_files = [f for f in section['film'] if isinstance(f, str) and f.endswith('.jpg')]
            else:
                # 文字列の場合（旧形式）
                film_files = [section['film']] if section['film'].endswith('.jpg') else []

            # 取得した jpg ファイルをダウンロード対象に追加
            for film in film_files:
                links.append((
                    "film",
                    build_asset_url(
                        asset_base_url,
                        f"{resource_path}/{film}"
                    )
                ))

        # `talk` の処理（ボイスファイル）
        if 'talk' in section:
            for part in section['talk']:
                if 'voice' in part:
                    links.append((
                        "voice",
                        build_asset_url(
                            asset_base_url,
                            f"{resource_path}/{part['voice']}"
                        )
                    ))

    # アセットをダウンロード
    return download_assets(
        links,
        data["resource_directory"],
        downloaded_ogg_files,
        scenario_id,
        metadata_filename
    )


# ---------------------------------------Start-------------------------------------------

def run_download_assets(
    data_directory: str,
    asset_base_url: str = DEFAULT_ASSET_BASE_URL
) -> dict:
    
    logging.info("Start download")
    
    if not os.path.exists(asset_folder):
        os.mkdir(asset_folder)

    downloaded_ogg_files = []
    totals = Counter()

    for scenario_type in os.listdir(data_directory):
        scenario_type_path = os.path.join(data_directory, scenario_type)
        if not os.path.isdir(scenario_type_path):
            continue

        for character in os.listdir(scenario_type_path):
            character_path = os.path.join(scenario_type_path, character)
            if not os.path.isdir(character_path):
                continue

            scenarios = os.listdir(character_path)
            for filename in scenarios:
                # JSON以外は無視
                if not filename.lower().endswith(".json"):
                    continue
                if '_script' in filename:
                    continue

                metadata_path = os.path.join(character_path, filename)
                scenario_id = os.path.splitext(filename)[0]
                logging.info(
                    "Reading scenario metadata: scenario_id=%s metadata=%s",
                    scenario_id,
                    filename
                )
                try:
                    with open(metadata_path, encoding="utf-8") as file:
                        data = json.load(file)

                    scenario_path = data.get('scenario_path')
                    if not isinstance(scenario_path, str):
                        raise ValueError("metadata has no scenario_path string")

                    if scenario_path.endswith('.ks'):
                        result = download_scenario_assets(
                            character,
                            scenario_type,
                            filename,
                            data,
                            data_directory,
                            downloaded_ogg_files,
                            asset_base_url,
                            scenario_id,
                            filename
                        )
                    else:
                        result = download_hscene_assets(
                            character,
                            scenario_type,
                            filename,
                            data,
                            data_directory,
                            downloaded_ogg_files,
                            asset_base_url,
                            scenario_id,
                            filename
                        )
                    totals.update(result)
                except Exception:
                    logging.exception(
                        "Scenario asset processing failed: scenario_id=%s "
                        "metadata=%s",
                        scenario_id,
                        filename
                    )
                    totals["failed"] += 1

    if len(ignore_links) != ignore_links_len:
        # Write new ignore links to file
        with open(ignore_file, 'a', encoding="utf-8") as f:
            for link in ignore_links[ignore_links_len:]:
                f.write(link + '\n')
    
    logging.info("Encoding %d ogg files...", len(downloaded_ogg_files))

    fix_ogg_files(list(set(downloaded_ogg_files)))

    success = totals["failed"] == 0
    message = (
        "Assets download completed"
        if success
        else "Assets download completed with failures"
    )
    logging.info(
        "Asset download summary: success=%s downloaded=%d failed=%d skipped=%d",
        success,
        totals["downloaded"],
        totals["failed"],
        totals["skipped"]
    )

    return {
        "success": success,
        "downloaded": totals["downloaded"],
        "failed": totals["failed"],
        "skipped": totals["skipped"],
        "ignored": len(ignore_links),
        "message": (
            f"{message}: {totals['downloaded']} downloaded, "
            f"{totals['failed']} failed, {totals['skipped']} skipped"
        )
    }
