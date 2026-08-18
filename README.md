# KP Downloader

KP Downloader is a tool for downloading and preparing Kamihime Project scenario data and assets for use with KamihimePlayer_Unity.

This project is unofficial and is not affiliated with or endorsed by the developers or publishers of Kamihime Project.

## Requirements

- Windows 10 / 11
- A valid session for the game
- Internet connection
- The required files included with the KP Downloader distribution

You do **not** need to install Python, KTX-Software, or FFmpeg separately when using the distributed version.

## Distribution Contents

A typical distribution should contain:

```text
KP Downloader.exe
setting.ini
latest.txt
tools/
    ktx.exe
    ktx.dll
    ffmpeg.exe
```

Additional files may be included depending on the version.

### Important: Do not remove files from `tools`

`ktx.exe` requires `ktx.dll`.

The distributed version is intended to work without a separate KTX-Software installation. If you receive an error such as:

```text
ktx.dll was not found
```

make sure that `ktx.dll` is present in the `tools` directory next to `ktx.exe`.

You should not need to add KTX-Software to your system PATH.

## Basic Usage

1. Extract the entire KP Downloader distribution.
2. Do not move or delete files inside the `tools` directory.
3. Start `KP Downloader.exe`.
4. Enter the required session information.
5. Start the download process.
6. Wait until all processing has completed.

The downloader performs several steps automatically:

```text
JSON download
    ↓
JSON processing
    ↓
Asset download
    ↓
KSD processing
    ↓
KSD asset extraction
    ↓
KTX2 → PNG conversion
    ↓
OGG processing
```

Depending on the scenario, not every step may be required.

## KSD / Harem 2.0

Newer Harem 2.0 scenarios may use `.ksd` files.

The downloader automatically searches for KSD files under the downloaded scenario directory and processes them.

KSD processing includes:

- Decryption
- Extraction of `gameData.json`
- Extraction of assets from `gameData.bin`
- KTX2 texture conversion
- OGG audio processing
- Scenario JSON preparation
- Removal of temporary files

No separate KSD extraction program is required.

## KTX Errors

If you see:

```text
ktx.dll cannot be found
```

this usually means that the KTX runtime files are missing from the distribution.

Please check:

```text
tools/
    ktx.exe
    ktx.dll
```

Both files are required.

Installing KTX-Software separately should normally not be necessary.

If the problem persists, please report the exact error message and the contents of your `tools` directory.

## FFmpeg Errors

The downloader uses the bundled `ffmpeg.exe` to process OGG files.

Make sure that:

```text
tools/ffmpeg.exe
```

exists.

A separate FFmpeg installation should normally not be necessary.

## `latest.txt`

`latest.txt` contains the ID ranges used by the downloader to determine which content should be checked.

Do not delete or rename this file.

If `latest.txt` is missing or invalid, the downloader may report something similar to:

```text
Kamihime ids to try: 0
```

This does not necessarily indicate a problem with the downloader itself. Check that `latest.txt` is present and contains valid data.

## Output Directory

The downloaded scenario data and assets are prepared for use with KamihimePlayer_Unity.

The exact directory structure depends on the scenario type and the downloaded content.

Do not manually move files while the downloader is running.

## Existing Files

The downloader may skip content that has already been downloaded or processed.

If you are troubleshooting a particular scenario, make sure you understand whether an existing file is being reused or a new download is being performed.

## Common Problems

### "ktx.dll was not found"

Check:

```text
tools/ktx.exe
tools/ktx.dll
```

### "ffmpeg.exe was not found"

Check:

```text
tools/ffmpeg.exe
```

### "Kamihime ids to try: 0"

Check `latest.txt`.

### The program starts but processing fails

Please provide:

- The exact error message
- The relevant log output
- The version of KP Downloader
- The scenario or character being processed
- A screenshot if a Windows error dialog appears

Do not send only a description such as "it doesn't work". The exact error message is much more useful for debugging.

## Before Asking for Help

Please check this README first.

When reporting a problem, please include the exact error message and, if possible, the relevant log output.

In particular, please check:

1. Is `latest.txt` present?
2. Is the `tools` directory present?
3. Is `ktx.exe` present?
4. Is `ktx.dll` present?
5. Is `ffmpeg.exe` present?
6. Are you running the latest version of KP Downloader?

This information will make troubleshooting significantly faster.

## KamihimePlayer_Unity

KP Downloader is designed to prepare downloaded content for use with the separate KamihimePlayer_Unity project.

KP Downloader and KamihimePlayer_Unity are separate projects and may have different requirements.

For questions specifically related to playback, Unity, Spine, scenario rendering, or the player itself, please refer to the documentation for KamihimePlayer_Unity.

## Important

Do not modify or delete files from the distribution unless you know what they are used for.

In particular, the following files are required by the downloader:

```text
setting.ini
latest.txt
tools/ktx.exe
tools/ktx.dll
tools/ffmpeg.exe
```

If you are distributing or copying the downloader to another computer, copy the **entire distribution**, not just the `.exe` file.

Thank you for using KP Downloader and for reporting bugs.
