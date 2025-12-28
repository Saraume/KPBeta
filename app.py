import os
import sys
import logging
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

import threading

from download_json_core import run_download_json
from download_assets_core import run_download_assets

def get_base_dir():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

BASE_DIR = get_base_dir()

ICON_DIR = os.path.join(BASE_DIR, "portrait")
ILLUST_DIR = os.path.join(BASE_DIR, "portrait_full")
LATEST_PATH = os.path.join(BASE_DIR, "latest.txt")

# -----------------------------
# GUI App
# -----------------------------

class DownloadApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("KPBeta Downloader")
        self.geometry("600x420")
        self.resizable(False, False)
        self._build_widgets()
        self._setup_logging()

    def _build_widgets(self):
        pad = {"padx": 10, "pady": 5}

        # ---- Session ----
        ttk.Label(self, text="x-kh-session").grid(row=0, column=0, sticky="w", **pad)
        self.session_entry = ttk.Entry(self, width=70)
        self.session_entry.grid(row=0, column=1, columnspan=2, **pad)

        # ---- Target ----
        ttk.Label(self, text="Download Target").grid(row=1, column=0, sticky="w", **pad)

        self.target_var = tk.StringVar(value="kamihime")
        targets = ["kamihime", "eidolon", "soul", "memorial", "burst", "all"]

        self.target_combo = ttk.Combobox(
            self, values=targets, textvariable=self.target_var, state="readonly", width=20
        )
        self.target_combo.grid(row=1, column=1, sticky="w", **pad)

        # ---- Save Root ----
        ttk.Label(self, text="Save Folder").grid(row=2, column=0, sticky="w", **pad)

        self.save_root_entry = ttk.Entry(self, width=50)
        self.save_root_entry.grid(row=2, column=1, sticky="w", **pad)
        default_root = self._get_default_save_root()
        self.save_root_entry.insert(0, default_root)

        ttk.Button(self, text="Browse...", command=self._browse_folder)\
            .grid(row=2, column=2, sticky="w", **pad)

        # ---- Options ----
        self.modify_json_var = tk.BooleanVar(value=True)
        self.modify_check = ttk.Checkbutton(
            self,
            text="Modify JSON (for Unity Player)",
            variable=self.modify_json_var
        )
        self.modify_check.grid(row=3, column=1, sticky="w", **pad)
        ttk.Button(
            self,
            text="Edit latest.txt",
            width=15,   # ← 文字数単位
            command=self.open_latest_editor
        ).grid(row=3, column=2, padx=0, pady=5, sticky="w")


        # ---- Run & Assets Buttons ----
        btn_frame = ttk.Frame(self)
        btn_frame.grid(row=4, column=0, columnspan=3, pady=5)

        self.run_button = ttk.Button(
            btn_frame,
            text="1. Download Scenario",
            width=20,
            command=self._on_run
        )
        self.run_button.pack(side="left", padx=5)

        self.assets_button = ttk.Button(
            btn_frame,
            text="2. Download Assets",
            width=20,
            command=self._on_run_assets
        )
        self.assets_button.pack(side="left", padx=5)

        # ---- Log / Result ----
        ttk.Label(self, text="Result").grid(row=5, column=0, sticky="nw", **pad)

        self.result_text = tk.Text(self, width=70, height=10, state="disabled")
        self.result_text.grid(row=5, column=1, columnspan=2, **pad)

    def _setup_logging(self):
        handler = TextHandler(self.result_text)
        formatter = logging.Formatter("[%(levelname)s] %(message)s")
        handler.setFormatter(formatter)

        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
        root_logger.setLevel(logging.INFO)
    
    def open_latest_editor(self):
        win = tk.Toplevel(self)
        win.title("Edit latest.txt")
        win.geometry("600x500")

        text = tk.Text(win, wrap="none")
        text.pack(fill="both", expand=True)

        # スクロールバー
        scrollbar = tk.Scrollbar(text)
        scrollbar.pack(side="right", fill="y")
        text.config(yscrollcommand=scrollbar.set)
        scrollbar.config(command=text.yview)

        # 読み込み
        if os.path.exists(LATEST_PATH):
            with open(LATEST_PATH, 'r', encoding='utf-8') as f:
                text.insert("1.0", f.read())

        def on_save():
            save_latest_txt(LATEST_PATH, text.get("1.0", "end-1c"))
            messagebox.showinfo("Saved", "latest.txt saved.")
            win.destroy()

        btn_frame = tk.Frame(win)
        btn_frame.pack(fill="x", pady=5)

        tk.Button(btn_frame, text="Save", command=on_save).pack(side="right", padx=5)
        tk.Button(btn_frame, text="Cancel", command=win.destroy).pack(side="right")


    # -----------------------------
    # Helpers
    # -----------------------------

    def _browse_folder(self):
        folder = filedialog.askdirectory()
        if folder:
            self.save_root_entry.delete(0, tk.END)
            self.save_root_entry.insert(0, folder)

    def _append_result(self, text):
        self.result_text.configure(state="normal")
        self.result_text.insert(tk.END, text + "\n")
        self.result_text.see(tk.END)
        self.result_text.configure(state="disabled")
    
    def _get_default_save_root(self):
        if getattr(sys, 'frozen', False):
            base_dir = os.path.dirname(sys.executable)
        else:
            base_dir = os.path.dirname(os.path.abspath(__file__))

        save_root = os.path.join(base_dir, "scenariosForBeta")
        os.makedirs(save_root, exist_ok=True)
        return save_root
    
    def _set_running(self, running: bool):
        state = "disabled" if running else "normal"

        # ---- ボタン ----
        self.run_button.config(state=state)
        self.assets_button.config(state=state)

        # ---- 入力欄 ----
        self.session_entry.config(state=state)
        self.save_root_entry.config(state=state)

        # ---- チェックボックス・タブなど ----
        self.modify_check.config(state=state)

        # ---- ステータス表示（あれば）----
        if hasattr(self, "status_label"):
            self.status_label.config(text="処理中..." if running else "待機中")

    
    # -----------------------------
    # Run logic
    # -----------------------------

    def _on_run(self):
        session = self.session_entry.get().strip()
        target = self.target_var.get()

        if target == "all":
            target_list = ["kamihime", "eidolon", "soul", "memorial", "burst"]
        else:
            target_list = [target]
        save_root = self.save_root_entry.get().strip()
        modify_json = self.modify_json_var.get()
        
        os.makedirs(ICON_DIR, exist_ok=True)
        os.makedirs(ILLUST_DIR, exist_ok=True)

        if not session:
            messagebox.showerror("Error", "x-kh-session is required.")
            return

        if not save_root:
            messagebox.showerror("Error", "Save folder is required.")
            return

        latest_dict = load_latest_txt(LATEST_PATH)

        # UI ロック
        self._set_running(True)

        self._append_result("Download started...")

        # スレッドで実行（GUIフリーズ防止）
        thread = threading.Thread(
            target=self._run_download_thread,
            args=(session, target_list, latest_dict, save_root, modify_json),
            daemon=True
        )

        thread.start()

    def _run_download_thread(self, session, target, latest_dict, save_root, modify_json):
        try:
            result = run_download_json(
                session=session,
                target=target,
                latest_dict=latest_dict,
                save_root=save_root,
                modify_json=modify_json
            )

            msg = result.get("message", "Completed.")
            success = result.get("success", True)

            self.after(0, self._append_result, msg)

            if success:
                self.after(0, messagebox.showinfo, "Completed", msg)
            else:
                self.after(0, messagebox.showwarning, "Completed with warnings", msg)

        except Exception as e:
            self.after(0, messagebox.showerror, "Fatal Error", str(e))

        finally:
            # ★ ここが最重要
            self.after(0, self._set_running, False)

    
    def _on_run_assets(self):
        data_directory = self.save_root_entry.get()

        if not os.path.isdir(data_directory):
            messagebox.showerror("Error", "Scenario folder not found")
            return

        # UI ロック
        self._set_running(True)

        # ログ初期化
        self._append_result("=== Start Asset Download ===")

        t = threading.Thread(
            target=self._run_assets_thread,
            args=(data_directory,),
            daemon=True
        )
        t.start()

    def _run_assets_thread(self, data_directory):
        try:
            result = run_download_assets(
                data_directory=data_directory
            )

            msg = result.get("message", "Assets download completed")

            self.after(0, self._append_result, msg)
            self.after(0, messagebox.showinfo, "Completed", msg)

        except Exception as e:
            self.after(0, messagebox.showerror, "Error", str(e))

        finally:
            self.after(0, self._set_running, False)


class TextHandler(logging.Handler):
    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget

    def emit(self, record):
        msg = self.format(record)

        # GUIスレッドに戻す
        self.text_widget.after(0, self._append, msg)

    def _append(self, msg):
        self.text_widget.configure(state="normal")
        self.text_widget.insert(tk.END, msg + "\n")
        self.text_widget.see(tk.END)
        self.text_widget.configure(state="disabled")

# -----------------------------
# latest editor
# -----------------------------
def load_latest_txt(path: str) -> dict:
    latest = {}
    if not os.path.exists(path):
        return latest

    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if ':' not in line:
                continue
            key, value = line.split(':', 1)
            latest[key.strip()] = value.strip()
    return latest

def save_latest_txt(path: str, text: str):
    with open(path, 'w', encoding='utf-8') as f:
        f.write(text)

# -----------------------------
# Entry point
# -----------------------------

if __name__ == "__main__":
    app = DownloadApp()
    app.mainloop()