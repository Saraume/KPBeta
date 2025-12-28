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
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(
    filename='1_error.log',
    filemode='w',
    level='INFO',
    format='[%(levelname)s] %(asctime)s: %(message)s'
)

base_url = dict()
base_url['fgimage'] = 'https://static-r.kamihimeproject.net/scenarios/fgimage/'
base_url['bgm'] = 'https://static-r.kamihimeproject.net/scenarios/bgm/'
base_url['bg'] = 'https://static-r.kamihimeproject.net/scenarios/bgimage/'
base_url['scenarios'] = 'https://static-r.kamihimeproject.net/scenarios/'

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
def download_script(script_path, url):
    s = get_asset_session()
    r = s.get(url, headers=headers, verify=False)
    if r.status_code == 200:
        try:
            with open(script_path, 'w', encoding='utf-8') as f:
                if script_path.endswith('json'):
                    content = re.sub('(\\],(?=\\s*\\}))', ']',
                                     re.sub('(\\},(?=\\s*\\]))', '}', r.text))
                    content = re.sub('(?!\\}),(?=\\s*\\})', '', content)
                    content = re.sub('\\];', ']', content)
                else:
                    content = r.text
                f.write(content)
                f.flush()
        except:
            os.remove(script_path)
        return True
    else:
        return False


def download_asset(link, resource_directory):
    link = link.replace(' ', '')
    folder = os.path.join(asset_folder, resource_directory)
    dst = os.path.join(folder, link[link.rfind('/')+1:]).replace('_pc_h', '')
    s = get_asset_session()

    if not os.path.exists(folder):
        os.mkdir(folder)

    if os.path.exists(dst):
        logging.warning('%s already exists' % dst)
    else:
        if link in ignore_links:
            logging.warning('Ignore %s' % link)
            return

        try:
            r = s.get(link, headers=headers, verify=False, timeout=req_timeout)
            if r.status_code == 200 and not r.text.startswith('<html>'):
                logging.info("Saved %s" % link)
                with open(dst, 'wb') as f:
                    for chunk in r:
                        f.write(chunk)
            else:
                logging.error("Error: %s" % link)
                logging.error("%s (%s)" % (link, r.status_code))

                if r.status_code == 404:
                    ignore_links.append(link)
        except requests.exceptions.RequestException as e:
            retry_links.append(link, resource_directory)
            logging.error("%s: %s" % (link, e))


def download_assets(links, resource_directory=''):
    with cf.ThreadPoolExecutor(max_workers=thread_num) as executor:
        futures = [executor.submit(download_asset, link, resource_directory) for link in links]

        for _ in cf.as_completed(futures):
            pass


def download_scenario_assets(character, scenario_type, filename, data, data_directory):
    script_file = '%s_script.ks' % filename.replace('.json', '').replace('.ks', '')
    script_path = os.path.join(data_directory, scenario_type, character, script_file)

    # Download script file if not exists
    if not os.path.exists(script_path):
        print ("Downloading script file...")
        if not download_script(script_path, base_url['scenarios'] + data['scenario_path']):
            print ("Failed to download script for %s" % filename)
            logging.error('Failed to download script for %s' % filename)
            return

    with open(script_path, encoding='utf-8') as file:
        script = file.read()

    links = []
    for match in re.finditer(r'\[chara_face.*storage="(.*)"', script):
        links.append(base_url['fgimage'] + match.group(1))

    for match in re.finditer(r'\[playbgm.*storage="(.*)"', script):
        links.append(base_url['bgm'] + match.group(1))

    for match in re.finditer(r'\[bg.*storage="(.*)"', script):
        links.append(base_url['bg'] + re.sub(r"(.*)(-.*)",
                                             r"\1_pc_h\2", match.group(1)))

    download_assets(links)

    links = []
    for match in re.finditer(r'\[playse.*storage="(.*)"', script):
        links.append(base_url['scenarios'] + '/'.join(
            data['scenario_path'].split('/')[:3]) + '/sound/' + match.group(1))

    download_assets(links, data['resource_directory'])


def download_hscene_assets(character, scenario_type, filename, data, data_directory):
    script_file = f"{filename.replace('.json', '').replace('.ks', '')}_script.json"
    script_path = os.path.join(data_directory, scenario_type, character, script_file)

    # スクリプトファイルがなければダウンロード
    if not os.path.exists(script_path):
        print("Downloading script file...")
        if not download_script(script_path, base_url['scenarios'] + data['scenario_path']):
            print(f"Failed to download script for {filename}")
            logging.error(f"Failed to download script for {filename}")
            return

    # JSONファイルを開く
    with open(script_path, encoding='utf-8') as file:
        script = json.load(file)

    links = []
    resource_path = data['scenario_path'][:data['scenario_path'].rfind('/')]
    scenes = script.get("scenario", [])

    for section in scenes:
        # `bgm` の処理
        if 'bgm' in section:
            links.append(f"{base_url['scenarios']}{resource_path}/{section['bgm']}")

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
                links.append(f"{base_url['scenarios']}{resource_path}/{film}")

        # `talk` の処理（ボイスファイル）
        if 'talk' in section:
            for part in section['talk']:
                if 'voice' in part:
                    links.append(f"{base_url['scenarios']}{resource_path}/{part['voice']}")

    # アセットをダウンロード
    download_assets(links, data['resource_directory'])


# ---------------------------------------Start-------------------------------------------

def run_download_assets(
    data_directory: str
) -> dict:
    

    logging.info("Start download")
    
    if not os.path.exists(asset_folder):
        os.mkdir(asset_folder)

    for scenario_type in os.listdir(data_directory):
        for character in os.listdir(os.path.join(data_directory, scenario_type)):
            scenarios = os.listdir(os.path.join(data_directory, scenario_type, character))
            for filename in scenarios:
                if '_script' in filename:
                    continue

                logging.info(character + " " + filename)

                with open(os.path.join(data_directory, scenario_type, character, filename), encoding="utf-8") as file:
                    data = json.load(file)

                if data['scenario_path'].endswith('.ks'):
                    download_scenario_assets(character, scenario_type, filename, data, data_directory)
                else:
                    download_hscene_assets(character, scenario_type, filename, data, data_directory)

    if len(ignore_links) != ignore_links_len:
        # Write new ignore links to file
        with open(ignore_file, 'a', encoding="utf-8") as f:
            for link in ignore_links[ignore_links_len:]:
                f.write(link + '\n')

    return {
        "success": True,
        "ignored": len(ignore_links),
        "message": "Assets download completed"
    }
