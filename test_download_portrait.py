import csv
import os
import tempfile
import unittest
from email.message import Message
from io import BytesIO
from unittest import mock
import urllib.error

from PIL import Image

import download_portrait
import download_english


class MockResponse(BytesIO):
    def __init__(self, content, status=200, content_type="image/jpeg"):
        super().__init__(content)
        self.status = status
        self.headers = Message()
        self.headers["Content-Type"] = content_type

    def getcode(self):
        return self.status


def jpeg_bytes():
    image = Image.new("RGB", (2, 2), color=(200, 30, 60))
    result = BytesIO()
    image.save(result, format="JPEG")
    return result.getvalue()


class DownloadImageTests(unittest.TestCase):
    def test_valid_jpeg_is_saved_after_http_200(self):
        content = jpeg_bytes()
        with tempfile.TemporaryDirectory() as temp_dir:
            destination = os.path.join(temp_dir, "portrait.jpg")
            with mock.patch.object(
                download_portrait.urllib.request,
                "urlopen",
                return_value=MockResponse(content)
            ) as urlopen:
                result = download_portrait.download_image(
                    "https://example.invalid/portrait.jpg",
                    destination
                )

            self.assertEqual(result["status"], "downloaded")
            self.assertEqual(result["http_status"], 200)
            self.assertEqual(result["content_type"], "image/jpeg")
            self.assertEqual(result["response_size"], len(content))
            self.assertEqual(urlopen.call_args.kwargs["timeout"], 30)
            with open(destination, "rb") as saved:
                self.assertEqual(saved.read(), content)

    def test_http_errors_record_status(self):
        for status in (403, 404):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as temp_dir:
                destination = os.path.join(temp_dir, "portrait.jpg")
                error = urllib.error.HTTPError(
                    "https://example.invalid/portrait.jpg",
                    status,
                    "denied",
                    Message(),
                    BytesIO(b"server error")
                )
                error.headers["Content-Type"] = "application/xml"
                with mock.patch.object(
                    download_portrait.urllib.request,
                    "urlopen",
                    side_effect=error
                ):
                    result = download_portrait.download_image(
                        error.url,
                        destination
                    )

                self.assertEqual(result["status"], "http_error")
                self.assertEqual(result["http_status"], status)
                self.assertEqual(result["content_type"], "application/xml")
                self.assertEqual(result["response_size"], len(b"server error"))
                self.assertFalse(os.path.exists(destination))

    def test_network_error_is_reported(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            destination = os.path.join(temp_dir, "portrait.jpg")
            with mock.patch.object(
                download_portrait.urllib.request,
                "urlopen",
                side_effect=urllib.error.URLError("offline")
            ):
                result = download_portrait.download_image(
                    "https://example.invalid/portrait.jpg",
                    destination
                )

            self.assertEqual(result["status"], "network_error")
            self.assertIn("offline", result["error"])
            self.assertFalse(os.path.exists(destination))

    def test_save_error_is_reported(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            destination = os.path.join(temp_dir, "portrait.jpg")
            with mock.patch.object(
                download_portrait.urllib.request,
                "urlopen",
                return_value=MockResponse(jpeg_bytes())
            ), mock.patch(
                "builtins.open",
                side_effect=PermissionError("read-only")
            ):
                result = download_portrait.download_image(
                    "https://example.invalid/portrait.jpg",
                    destination
                )

            self.assertEqual(result["status"], "save_error")
            self.assertIn("read-only", result["error"])
            self.assertFalse(os.path.exists(destination))

    def test_existing_file_is_not_overwritten_or_requested(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            destination = os.path.join(temp_dir, "portrait.jpg")
            with open(destination, "wb") as existing:
                existing.write(b"keep")

            with mock.patch.object(
                download_portrait.urllib.request,
                "urlopen"
            ) as urlopen:
                result = download_portrait.download_image(
                    "https://example.invalid/portrait.jpg",
                    destination
                )

            self.assertEqual(result["status"], "already_exists")
            urlopen.assert_not_called()
            with open(destination, "rb") as existing:
                self.assertEqual(existing.read(), b"keep")

    def test_invalid_jpeg_is_not_saved(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            destination = os.path.join(temp_dir, "portrait.jpg")
            with mock.patch.object(
                download_portrait.urllib.request,
                "urlopen",
                return_value=MockResponse(
                    b"<html>not an image</html>",
                    content_type="text/html"
                )
            ):
                result = download_portrait.download_image(
                    "https://example.invalid/portrait.jpg",
                    destination
                )

            self.assertEqual(result["status"], "invalid_image")
            self.assertEqual(result["http_status"], 200)
            self.assertFalse(os.path.exists(destination))


class PortraitUrlTests(unittest.TestCase):
    def test_dmm_url_generation_keeps_existing_key_and_host(self):
        plain_text, encrypted, relative_path, url = (
            download_portrait.build_url_details(
                "corecard_chara_",
                "7"
            )
        )

        self.assertEqual(plain_text, "corecard_chara_7_0")
        self.assertEqual(
            encrypted,
            "2c93eebb4a79e124b8ca20ec4e2f4c24745104373ca4708d"
        )
        self.assertEqual(
            relative_path,
            f"{encrypted[-6:-3]}/{encrypted[-3:]}/{encrypted}.jpg"
        )
        self.assertEqual(
            url,
            "https://static-r.kamihimeproject.net/resources/pc/normal/"
            + relative_path
        )

    def test_dmm_harem_url_generation_keeps_existing_suffix_and_format(self):
        plain_text, encrypted, relative_path, url = (
            download_portrait.build_url_details(
                "questimg_harem",
                "example"
            )
        )

        self.assertEqual(plain_text, "questimg_haremexample_1")
        self.assertEqual(
            encrypted,
            download_portrait.kamihime_encrypt(plain_text)
        )
        self.assertTrue(relative_path.endswith(".png"))
        self.assertTrue(
            url.startswith(
                "https://static-r.kamihimeproject.net/resources/pc/normal/"
            )
        )

    def test_nutaku_url_generation_uses_nutaku_key_and_host(self):
        plain_text, encrypted, relative_path, url = (
            download_portrait.build_url_details(
                "corecard_chara_",
                "7",
                service="Nutaku"
            )
        )

        self.assertEqual(plain_text, "corecard_chara_7_0")
        self.assertEqual(
            encrypted,
            "8b12ce67eb09c39dc2d177d520717afdc0fb6cb2ce0eb99a"
        )
        self.assertEqual(
            relative_path,
            f"{encrypted[-6:-3]}/{encrypted[-3:]}/{encrypted}.jpg"
        )
        self.assertEqual(
            url,
            "https://gnkh-resource-r.prod.nkh.dmmgames.com/"
            "resources/pc/normal/" + relative_path
        )

    def test_nutaku_download_covers_all_portrait_rules(self):
        expected = {
            "kamihime": (
                "corecard_chara_0007_0",
                "illustzoom_chara_0007_0",
            ),
            "eidolon": (
                "corecard_summon_0042_0",
                "illustzoom_summon_0042_0",
            ),
            "soul": (
                "corecard_job_0009_0",
                "illustzoom_job_0009_0",
            ),
            "memorial": ("corecard_item_30007",),
            "burst": ("corecard_item_32007",),
        }
        ids = {
            "kamihime": 7,
            "eidolon": 42,
            "soul": 9,
            "memorial": 7,
            "burst": 7,
        }
        calls = []

        def fake_download(url, save_path, expected_format):
            calls.append((url, save_path, expected_format))
            return {
                "status": "downloaded",
                "request_url": url,
                "http_status": 200,
                "content_type": f"image/{expected_format.lower()}",
                "response_size": 123,
                "save_path": save_path,
                "error": "",
            }

        with mock.patch.object(
            download_portrait,
            "download_image",
            side_effect=fake_download
        ):
            for char_type, expected_plain_texts in expected.items():
                with self.subTest(char_type=char_type):
                    records = download_portrait.download_portrait(
                        char_type,
                        ids[char_type],
                        f"Nutaku_{char_type}",
                        service="Nutaku"
                    )

                    self.assertEqual(
                        [record["plain_text"] for record in records],
                        list(expected_plain_texts)
                    )
                    self.assertTrue(all(
                        record["request_url"].startswith(
                            download_portrait.NUTAKU_IMAGE_BASE_URL
                        )
                        for record in records
                    ))
                    for record in records:
                        self.assertEqual(record["status"], "downloaded")
                        self.assertIn(
                            f"/{record['encrypted']}.",
                            record["request_url"]
                        )
                        self.assertEqual(
                            record["relative_path"],
                            f"{record['encrypted'][-6:-3]}/"
                            f"{record['encrypted'][-3:]}/"
                            f"{record['encrypted']}."
                            f"{'png' if record['portrait_type'] == 'illust' else 'jpg'}"
                        )

        self.assertEqual(
            [call[2] for call in calls],
            ["JPEG", "PNG", "JPEG", "PNG", "JPEG", "PNG", "JPEG", "JPEG"]
        )
        self.assertTrue(all(
            path.startswith(download_portrait.ICON_DIR)
            for _, path, image_format in calls
            if image_format == "JPEG"
        ))
        self.assertEqual(
            sum(image_format == "PNG" for _, _, image_format in calls),
            3
        )

    def test_portrait_ids_are_zero_padded_before_encryption_for_both_services(self):
        cases = (
            ("kamihime", 1, ("corecard_chara_0001_0", "illustzoom_chara_0001_0")),
            ("kamihime", 12, ("corecard_chara_0012_0", "illustzoom_chara_0012_0")),
            ("kamihime", 123, ("corecard_chara_0123_0", "illustzoom_chara_0123_0")),
            ("kamihime", 1234, ("corecard_chara_1234_0", "illustzoom_chara_1234_0")),
            ("memorial", 1, ("corecard_item_30001",)),
            ("burst", 1, ("corecard_item_32001",)),
            ("memorial", 10001, ("corecard_item_40001",)),
        )

        with mock.patch.object(
            download_portrait,
            "download_image",
            return_value={"status": "mocked"}
        ):
            for service in ("DMM", "Nutaku"):
                for char_type, char_id, expected_plain_texts in cases:
                    with self.subTest(
                        service=service,
                        char_type=char_type,
                        char_id=char_id
                    ):
                        records = download_portrait.download_portrait(
                            char_type,
                            char_id,
                            f"{service}_{char_type}_{char_id}",
                            service=service
                        )
                        self.assertEqual(
                            [record["plain_text"] for record in records],
                            list(expected_plain_texts)
                        )

    def test_nutaku_download_records_failed_image_result(self):
        def fake_failed_download(url, save_path, expected_format):
            return {
                "status": "http_error",
                "request_url": url,
                "http_status": 404,
                "content_type": "text/plain",
                "response_size": 0,
                "save_path": save_path,
                "error": "HTTP Error 404",
            }

        with mock.patch.object(
            download_portrait,
            "download_image",
            side_effect=fake_failed_download
        ):
            records = download_portrait.download_portrait(
                "memorial",
                7,
                "Missing",
                service="Nutaku"
            )

        self.assertEqual(records[0]["status"], "http_error")
        self.assertEqual(records[0]["portrait_status"], "http_error")
        self.assertEqual(records[0]["http_status"], 404)
        self.assertIn(
            download_portrait.NUTAKU_IMAGE_BASE_URL,
            records[0]["request_url"]
        )


class PortraitAuditTests(unittest.TestCase):
    def test_portrait_audit_contains_encryption_url_and_result(self):
        def fake_download(url, save_path, expected_format):
            return {
                "status": "downloaded",
                "request_url": url,
                "http_status": 200,
                "content_type": f"image/{expected_format.lower()}",
                "response_size": 123,
                "save_path": save_path,
                "error": "",
            }

        with mock.patch.object(
            download_portrait,
            "download_image",
            side_effect=fake_download
        ):
            records = download_portrait.download_portrait(
                "kamihime",
                7,
                "Name",
                rank="SSR"
            )

        self.assertEqual(
            [record["portrait_type"] for record in records],
            ["icon", "illust"]
        )
        self.assertEqual(records[0]["character_id"], 7)
        self.assertEqual(records[0]["character_name"], "Name")
        self.assertEqual(records[0]["rank"], "SSR")
        self.assertEqual(records[0]["plain_text"], "corecard_chara_0007_0")
        self.assertTrue(records[0]["encrypted"])
        self.assertTrue(records[0]["relative_path"].endswith(".jpg"))
        self.assertEqual(
            records[0]["request_url"],
            download_portrait.build_url("corecard_chara_", "0007")
        )
        self.assertEqual(records[0]["status"], "downloaded")
        self.assertEqual(
            records[1]["request_url"],
            download_portrait.build_url("illustzoom_chara_", "0007")
        )

    def test_character_info_failure_is_audited_without_portrait_request(self):
        with mock.patch.object(
            download_english,
            "get_json_result",
            return_value=(None, "HTTP 403")
        ), mock.patch.object(download_english, "download_portrait") as portrait:
            row, records = download_english.process_kamihime(
                1,
                mock.Mock(),
                {},
                "unused"
            )

        self.assertIsNone(row)
        portrait.assert_not_called()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["character_id"], 1)
        self.assertEqual(records[0]["status"], "character_info_failed")
        self.assertEqual(records[0]["portrait_status"], "not_attempted")
        self.assertEqual(records[0]["http_status"], "403")
        self.assertEqual(records[0]["error"], "HTTP 403")

    def test_audit_csv_uses_separate_schema(self):
        row = {
            "character_id": 1,
            "character_name": "",
            "status": "character_info_failed",
            "portrait_status": "not_attempted",
            "error": "HTTP 403",
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, "portrait_audit.csv")
            download_english.write_portrait_audit([row], path)
            with open(path, newline="", encoding="utf-8") as audit_file:
                reader = csv.DictReader(audit_file)
                saved_rows = list(reader)
                fields = reader.fieldnames

        self.assertEqual(fields, download_english.PORTRAIT_AUDIT_FIELDS)
        self.assertNotIn("character_id", download_english.CSV_FIELDS)
        self.assertEqual(saved_rows[0]["character_id"], "1")
        self.assertEqual(saved_rows[0]["status"], "character_info_failed")
        self.assertEqual(saved_rows[0]["portrait_status"], "not_attempted")
        self.assertEqual(saved_rows[0]["error"], "HTTP 403")


if __name__ == "__main__":
    unittest.main()
