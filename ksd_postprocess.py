import gzip
import json
import logging
import os
import struct
import shutil
import subprocess
import sys

KEY = bytes([
    0,1,17,33,0,1,17,33,16,2,18,161,0,1,17,33,
    32,3,19,177,0,1,17,33,48,4,20,193,0,1,17,33,
    64,5,21,209,0,1,17,33,80,6,22,225,0,1,17,33,
    96,7,23,241,0,1,17,33,112,8,24,1,0,1,17,33
])

# process_root()
# process_ksd()
# get_resource_directory()
# decrypt_ksd()
# extract_assets()
# rename_scenario_json()
# cleanup_files()
# convert_ktx2()
# fix_ogg()

def get_runtime_dir():
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)

    return os.path.dirname(os.path.abspath(__file__))

BASE_DIR = get_runtime_dir()

KTX_EXE = os.path.join(
    BASE_DIR,
    "tools",
    "ktx.exe"
)

FFMPEG_EXE = os.path.join(
    BASE_DIR,
    "tools",
    "ffmpeg.exe"
)

def process_root(
    save_root: str,
    assets_root: str
):
    """
    scenarioForBeta配下を探索して
    *_script.ksd を処理する
    """

    logging.info(
        "Scanning KSD files under %s",
        save_root
    )

    count = 0

    for root, dirs, files in os.walk(save_root):

        for file in files:

            if not file.endswith(".ksd"):
                continue

            ksd_path = os.path.join(
                root,
                file
            )

            try:
                process_ksd(
                    ksd_path,
                    assets_root
                )

                count += 1

            except Exception:

                logging.exception(
                    "Failed to process %s",
                    ksd_path
                )

    logging.info(
        "Processed %d KSD files",
        count
    )

def process_ksd(
    ksd_path: str,
    assets_root: str
):

    metadata_path = (
        ksd_path
        .replace("_script.ksd", ".json")
    )

    resource_directory = (
        get_resource_directory(
            metadata_path
        )
    )

    output_dir = os.path.join(
        assets_root,
        resource_directory
    )

    os.makedirs(
        output_dir,
        exist_ok=True
    )

    game_json_path, game_bin_path = (
        decrypt_ksd(
            ksd_path,
            output_dir
        )
    )

    transcode_files, ogg_files = extract_assets(
        game_json_path,
        game_bin_path,
        output_dir
    )

    convert_ktx2(transcode_files)
    fix_ogg_files(ogg_files)

    rename_scenario_json(
        game_json_path,
        ksd_path
    )

    cleanup_files(
        game_bin_path,
        ksd_path
    )

def get_resource_directory(
    metadata_path: str
) -> str:

    with open(
        metadata_path,
        encoding="utf-8"
    ) as f:

        data = json.load(f)

    return data[
        "resource_directory"
    ]

def decrypt_ksd(
    ksd_path: str,
    output_dir: str
):
    """
    ksd
      ↓ xor
      ↓ gzip
      ↓ split
    gameData.json
    gameData.bin
    """

    logging.info(
        "Decrypting KSD: %s",
        os.path.basename(ksd_path)
    )

    with open(ksd_path, "rb") as f:
        encrypted = f.read()

    decoded = bytes(
        b ^ KEY[i % len(KEY)]
        for i, b in enumerate(encrypted)
    )

    decompressed = gzip.decompress(decoded)

    _, json_off, json_size, bin_off, bin_size = struct.unpack(
        "<IIIII",
        decompressed[:20]
    )

    json_bytes = decompressed[
        json_off:
        json_off + json_size
    ]

    bin_data = decompressed[
        bin_off:
        bin_off + bin_size
    ]

    game_json_path = os.path.join(
        output_dir,
        "gameData.json"
    )

    game_bin_path = os.path.join(
        output_dir,
        "gameData.bin"
    )

    with open(
        game_json_path,
        "wb"
    ) as f:
        f.write(json_bytes)

    with open(
        game_bin_path,
        "wb"
    ) as f:
        f.write(bin_data)

    logging.info(
        "Extracted gameData.json and gameData.bin"
    )

    return (
        game_json_path,
        game_bin_path
    )

def extract_assets(
    game_json_path: str,
    game_bin_path: str,
    output_dir: str
):
    transcode_files = []
    ogg_files = []

    logging.info(
        "Extracting assets from %s",
        os.path.basename(game_json_path)
    )

    with open(
        game_json_path,
        encoding="utf-8"
    ) as f:
        game_data = json.load(f)

    with open(
        game_bin_path,
        "rb"
    ) as f:
        bin_data = f.read()

    def extract(asset):

        path = asset["path"]
        offset = asset["offset"]
        size = asset["size"]

        data = bin_data[
            offset:
            offset + size
        ]

        out_path = os.path.join(
            output_dir,
            path
        )

        os.makedirs(
            os.path.dirname(out_path),
            exist_ok=True
        )

        with open(
            out_path,
            "wb"
        ) as f:
            f.write(data)
        
        if asset.get("needTranscoding"):
            transcode_files.append(out_path)
        
        if out_path.lower().endswith(".ogg"):
            ogg_files.append(out_path)

    for asset in game_data["assets"]:

        # 実体を持つassetだけ抽出
        if asset.get("size", 0) > 0:
            extract(asset)

        for sub in asset.get("subAssets", []):
            extract(sub)

        logging.info(
            "Assets extracted to %s",
            output_dir
        )

    return transcode_files, ogg_files

def convert_ktx2(
    transcode_files
):

    if not transcode_files:
        return

    if not os.path.exists(KTX_EXE):

        logging.warning(
            "ktx.exe not found: %s",
            KTX_EXE
        )

        return

    logging.info(
        "Converting %d KTX2 textures",
        len(transcode_files)
    )

    for png_path in transcode_files:

        try:

            ktx2_path = (
                os.path.splitext(
                    png_path
                )[0]
                + ".ktx2"
            )

            os.rename(
                png_path,
                ktx2_path
            )

            subprocess.run(
                [
                    KTX_EXE,
                    "extract",
                    ktx2_path,
                    "--output",
                    png_path
                ],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )

            if os.path.exists(
                ktx2_path
            ):
                os.remove(
                    ktx2_path
                )

            logging.info(
                "Converted: %s",
                os.path.basename(
                    png_path
                )
            )
            

        except Exception:

            logging.exception(
                "Failed to convert %s",
                png_path
            )

def fix_ogg_files(ogg_files):

    if not ogg_files:
        return
    
    if not os.path.exists(
        FFMPEG_EXE
    ):

        logging.warning(
            "ffmpeg.exe not found: %s",
            FFMPEG_EXE
        )

        return

    logging.info(
        "Fixing %d ogg files",
        len(ogg_files)
    )

    for ogg_path in ogg_files:

        try:

            tmp_path = (
                os.path.splitext(
                    ogg_path
                )[0]
                + "_tmp.ogg"
            )

            subprocess.run(
                [
                    FFMPEG_EXE,
                    "-y",
                    "-i",
                    ogg_path,
                    "-c:a",
                    "libvorbis",
                    tmp_path
                ],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )

            if os.path.exists(
                tmp_path
            ):

                os.replace(
                    tmp_path,
                    ogg_path
                )

            logging.info(
                "Fixed: %s",
                os.path.basename(
                    ogg_path
                )
            )

        except Exception:

            logging.exception(
                "Failed to fix ogg: %s",
                ogg_path
            )

def rename_scenario_json(
    game_json_path,
    ksd_path
):

    dst_json = (
        os.path.splitext(
            ksd_path
        )[0]
        + ".json"
    )

    shutil.move(
        game_json_path,
        dst_json
    )

    return dst_json

def cleanup_files(
    game_bin_path,
    ksd_path
):

    try:
        os.remove(
            game_bin_path
        )
    except:
        pass

    try:
        os.remove(
            ksd_path
        )
    except:
        pass