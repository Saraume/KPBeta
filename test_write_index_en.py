import csv
import os
import tempfile
import unittest
from unittest import mock

import app
import download_english


class WriteIndexEnglishTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.temp_dir.name, "index_en.csv")

    def tearDown(self):
        self.temp_dir.cleanup()

    @staticmethod
    def make_row(name, rank, info=""):
        return {
            "Name": name,
            "Rank": rank,
            "Info": info,
        }

    def read_rows(self):
        with open(self.path, "r", encoding="utf-8", newline="") as file:
            reader = csv.DictReader(file)
            return reader.fieldnames, list(reader)

    def test_creates_csv_with_header_and_normalized_values(self):
        download_english.write_index_en(
            [self.make_row("  New  Unit ", "SSR", "  description  ")],
            self.path,
        )

        fields, rows = self.read_rows()
        self.assertEqual(fields, download_english.CSV_FIELDS)
        self.assertEqual(rows[0]["Name"], "New Unit")
        self.assertEqual(rows[0]["Info"], "description")

    def test_appends_new_record_to_existing_csv(self):
        download_english.write_index_en(
            [self.make_row("First", "SR")],
            self.path,
        )

        download_english.write_index_en(
            [self.make_row("Second", "SSR")],
            self.path,
        )

        _, rows = self.read_rows()
        self.assertEqual([row["Name"] for row in rows], ["First", "Second"])

    def test_does_not_append_existing_rank_and_name(self):
        download_english.write_index_en(
            [self.make_row("Same", "SSR", "original")],
            self.path,
        )

        download_english.write_index_en(
            [self.make_row("Same", "SSR", "updated")],
            self.path,
        )

        _, rows = self.read_rows()
        self.assertEqual(len(rows), 1)

    def test_existing_record_bytes_are_unchanged(self):
        download_english.write_index_en(
            [self.make_row("Original", "SR", "original text")],
            self.path,
        )
        with open(self.path, "rb") as file:
            original_bytes = file.read()

        download_english.write_index_en(
            [self.make_row("Added", "SSR")],
            self.path,
        )

        with open(self.path, "rb") as file:
            self.assertTrue(file.read().startswith(original_bytes))

    def test_empty_download_leaves_existing_file_unchanged(self):
        download_english.write_index_en(
            [self.make_row("Original", "SR")],
            self.path,
        )
        with open(self.path, "rb") as file:
            original_bytes = file.read()

        download_english.write_index_en([], self.path)

        with open(self.path, "rb") as file:
            self.assertEqual(file.read(), original_bytes)

    def test_deduplicates_records_within_one_call(self):
        download_english.write_index_en(
            [
                self.make_row("Repeated", "SSR", "first"),
                self.make_row("Repeated", "SSR", "second"),
            ],
            self.path,
        )

        _, rows = self.read_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["Info"], "first")

    def test_header_is_written_only_once(self):
        download_english.write_index_en(
            [self.make_row("First", "SR")],
            self.path,
        )
        download_english.write_index_en(
            [self.make_row("Second", "SSR")],
            self.path,
        )

        with open(self.path, "r", encoding="utf-8", newline="") as file:
            header_lines = [
                line for line in file
                if line.rstrip("\r\n") == ",".join(download_english.CSV_FIELDS)
            ]
        self.assertEqual(len(header_lines), 1)

    def test_read_failure_does_not_modify_existing_file(self):
        download_english.write_index_en(
            [self.make_row("Original", "SR")],
            self.path,
        )
        with open(self.path, "rb") as file:
            original_bytes = file.read()

        original_open = open

        def fail_when_reading_existing(path, mode="r", *args, **kwargs):
            if os.fspath(path) == self.path and "r" in mode:
                raise OSError("read failed")
            return original_open(path, mode, *args, **kwargs)

        with mock.patch("builtins.open", side_effect=fail_when_reading_existing):
            with self.assertRaises(OSError):
                download_english.write_index_en(
                    [self.make_row("Added", "SSR")],
                    self.path,
                )

        with open(self.path, "rb") as file:
            self.assertEqual(file.read(), original_bytes)

    def test_english_index_is_written_to_execution_directory(self):
        scenario_dir = os.path.join(self.temp_dir.name, "scenariosForBeta")
        os.mkdir(scenario_dir)

        with mock.patch.object(download_english, "BASE_DIR", self.temp_dir.name), \
                mock.patch.object(download_english, "load_latest", return_value={}), \
                mock.patch.object(download_english, "create_session", return_value=(None, {})), \
                mock.patch.object(download_english, "generate_kamihime_ids", return_value=["id"]), \
                mock.patch.object(
                    download_english,
                    "process_kamihime",
                    return_value=(self.make_row("Unit", "SSR"), []),
                ), \
                mock.patch.object(download_english.modifi_json, "process_root"):
            result = download_english.run_download_english(
                session="session",
                save_root=scenario_dir,
                latest_path=os.path.join(self.temp_dir.name, "missing-latest.txt"),
                targets=["kamihime"],
            )

        self.assertEqual(
            result["index"],
            os.path.join(self.temp_dir.name, "index_en.csv"),
        )
        self.assertTrue(os.path.isfile(result["index"]))
        self.assertFalse(os.path.exists(os.path.join(scenario_dir, "index_en.csv")))

    def test_existing_index_prompts_and_declining_stops_download(self):
        app_instance = app.DownloadApp.__new__(app.DownloadApp)
        with open(self.path, "w", encoding="utf-8") as file:
            file.write("existing")

        with mock.patch.object(app, "BASE_DIR", self.temp_dir.name), \
                mock.patch.object(app.messagebox, "askyesno", return_value=False) as ask:
            app_instance._on_run()

        ask.assert_called_once_with(
            "Index File Exists",
            "An index file already exists. Do you want to continue?",
        )


if __name__ == "__main__":
    unittest.main()
