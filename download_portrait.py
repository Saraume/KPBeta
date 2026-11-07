import http.client
import os
import sys
import urllib.error
import urllib.request
from io import BytesIO

from Crypto.Cipher import Blowfish
from Crypto.Util.Padding import pad
from PIL import Image, UnidentifiedImageError


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

DMM_ENCRYPTION_KEY = b"bLoWfIsH"
DMM_IMAGE_BASE_URL = (
    "https://static-r.kamihimeproject.net/resources/pc/normal/"
)
NUTAKU_ENCRYPTION_KEY = b"WD24kYA7UaiHMpNq6BQ"
NUTAKU_IMAGE_BASE_URL = (
    "https://gnkh-resource-r.prod.nkh.dmmgames.com/resources/pc/normal/"
)

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

def _encrypt(data: str, key: bytes) -> str:
    cipher = Blowfish.new(key, Blowfish.MODE_ECB)
    padded = pad(data.encode("utf-8"), 8)
    return cipher.encrypt(padded).hex()


def kamihime_encrypt(data: str) -> str:
    return _encrypt(data, DMM_ENCRYPTION_KEY)


def get_path(t: str, p: str) -> str:
    r = t.rfind(".")
    e = t[:r] if r != -1 else t
    part1 = e[-6:-3]
    part2 = e[-3:]
    ext = ".png" if ("illust" in p or "harem" in p) else ".jpg"
    return f"{part1}/{part2}/{t}{ext}"


def build_url_details(type_str: str, x: str, service: str = "DMM"):
    if "corecard_item_" in type_str:
        data = type_str + x
    elif "questimg_harem" in type_str:
        data = type_str + x + "_1"
    else:
        data = type_str + x + "_0"

    service_config = {
        "DMM": (DMM_ENCRYPTION_KEY, DMM_IMAGE_BASE_URL),
        "Nutaku": (NUTAKU_ENCRYPTION_KEY, NUTAKU_IMAGE_BASE_URL),
    }.get(service)
    if service_config is None:
        raise ValueError(f"unsupported service: {service}")

    encryption_key, image_base_url = service_config
    final = _encrypt(data, encryption_key)
    path = get_path(final, type_str)
    url = image_base_url + path
    return data, final, path, url


def build_url(type_str: str, x: str, service: str = "DMM") -> str:
    return build_url_details(type_str, x, service)[3]


# https://gnkh-resource-r.prod.nkh.dmmgames.com/resources/pc/normal/
# https://gnkh-resource-r.prod.nkh.dmmgames.com/resources/pc/normal/1ef/750/8b12ce67eb09c39d5eab986c2a64547a510d683a1d1ef750.jpg
# https://static-r.kamihimeproject.net/resources/pc/normal/75d/25c/2c93eebb4a79e124bc0f15fd6ea534d9169c384ae475d25c.jpg

# =============================
# ダウンロード処理
# =============================

def _image_result(
    status,
    url,
    save_path,
    http_status=None,
    content_type="",
    response_size=0,
    error=""
):
    return {
        "status": status,
        "request_url": url,
        "http_status": http_status,
        "content_type": content_type,
        "response_size": response_size,
        "save_path": save_path,
        "error": error,
    }


def _valid_image(content: bytes, expected_format: str) -> bool:
    try:
        with Image.open(BytesIO(content)) as image:
            if image.format != expected_format:
                return False
            image.load()
    except (
        OSError,
        ValueError,
        UnidentifiedImageError,
        Image.DecompressionBombError
    ):
        return False
    return True


def download_image(
    url: str,
    save_path: str,
    expected_format: str = "JPEG"
) -> dict:
    if os.path.isfile(save_path):
        return _image_result(
            "already_exists",
            url,
            save_path,
            error="destination already exists"
        )

    try:
        response = urllib.request.urlopen(url, timeout=30)
    except urllib.error.HTTPError as error:
        content_type = (
            error.headers.get("Content-Type", "")
            if error.headers
            else ""
        )
        try:
            response_size = len(error.read())
        except (OSError, http.client.HTTPException):
            response_size = 0
        return _image_result(
            "http_error",
            url,
            save_path,
            error.code,
            content_type,
            response_size,
            str(error)
        )
    except (
        urllib.error.URLError,
        TimeoutError,
        OSError,
        http.client.HTTPException
    ) as error:
        return _image_result(
            "network_error",
            url,
            save_path,
            error=str(error)
        )

    http_status = None
    content_type = ""
    try:
        with response:
            http_status = response.getcode()
            content_type = response.headers.get("Content-Type", "")
            content = response.read()
    except (
        urllib.error.URLError,
        TimeoutError,
        OSError,
        http.client.HTTPException
    ) as error:
        return _image_result(
            "network_error",
            url,
            save_path,
            http_status,
            content_type,
            error=str(error)
        )

    response_size = len(content)
    if http_status != 200:
        return _image_result(
            "http_error",
            url,
            save_path,
            http_status,
            content_type,
            response_size,
            f"HTTP {http_status}"
        )

    if response_size == 0:
        return _image_result(
            "invalid_image",
            url,
            save_path,
            http_status,
            content_type,
            response_size,
            "empty image response"
        )

    if not _valid_image(content, expected_format):
        return _image_result(
            "invalid_image",
            url,
            save_path,
            http_status,
            content_type,
            response_size,
            f"response is not a valid {expected_format} image"
        )

    created_file = False
    try:
        os.makedirs(os.path.dirname(save_path) or ".", exist_ok=True)
        image_file = open(save_path, "xb")
        created_file = True
        with image_file:
            written = image_file.write(content)
            if written != response_size:
                raise OSError(
                    f"wrote {written} of {response_size} bytes"
                )
    except FileExistsError as error:
        if os.path.isfile(save_path):
            return _image_result(
                "already_exists",
                url,
                save_path,
                http_status,
                content_type,
                response_size,
                "destination already exists"
            )
        return _image_result(
            "save_error",
            url,
            save_path,
            http_status,
            content_type,
            response_size,
            str(error)
        )
    except OSError as error:
        if created_file:
            try:
                os.remove(save_path)
            except OSError as cleanup_error:
                error = OSError(
                    f"{error}; partial-file cleanup failed: {cleanup_error}"
                )
        return _image_result(
            "save_error",
            url,
            save_path,
            http_status,
            content_type,
            response_size,
            str(error)
        )

    return _image_result(
        "downloaded",
        url,
        save_path,
        http_status,
        content_type,
        response_size
    )

# =============================
# 外部API（0_download_json から呼ぶ）
# =============================

def download_portrait(
    char_type: str,
    char_id: int,
    char_name: str,
    rank: str = "",
    service: str = "DMM",
):
    rule = PORTRAIT_RULES.get(char_type)
    if not rule:
        return [{
            "character_id": char_id,
            "character_name": char_name,
            "rank": rank,
            "portrait_type": "",
            "status": "not_attempted",
            "portrait_status": "not_attempted",
            "plain_text": "",
            "encrypted": "",
            "relative_path": "",
            "request_url": "",
            "http_status": "",
            "content_type": "",
            "response_size": 0,
            "save_path": "",
            "error": f"unsupported character type: {char_type}",
        }]

    real_id = char_id + rule["id_offset"]
    image_id = f"{real_id:04d}"
    results = []

    # --- アイコン ---
    plain_text, encrypted, relative_path, icon_url = build_url_details(
        rule["icon"],
        image_id,
        service
    )
    icon_path = os.path.join(ICON_DIR, f"{char_name}.jpg")
    result = download_image(icon_url, icon_path, "JPEG")
    results.append({
        "character_id": char_id,
        "character_name": char_name,
        "rank": rank,
        "portrait_type": "icon",
        "plain_text": plain_text,
        "encrypted": encrypted,
        "relative_path": relative_path,
        **result,
        "portrait_status": result["status"],
    })

    # --- 立ち絵（必要なタイプのみ） ---
    if rule["illust"]:
        plain_text, encrypted, relative_path, illust_url = build_url_details(
            rule["illust"],
            image_id,
            service
        )
        illust_path = os.path.join(ILLUST_DIR, f"{char_name}.png")
        result = download_image(illust_url, illust_path, "PNG")
        results.append({
            "character_id": char_id,
            "character_name": char_name,
            "rank": rank,
            "portrait_type": "illust",
            "plain_text": plain_text,
            "encrypted": encrypted,
            "relative_path": relative_path,
            **result,
            "portrait_status": result["status"],
        })

    return results
