import base64
import hashlib
import json
import platform
import textwrap
import uuid
from datetime import datetime, timezone
from pathlib import Path
from tkinter import messagebox, ttk
import tkinter as tk
from typing import Dict, Optional, Tuple

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey


LICENSE_FILE = Path("license.json")
PUBLIC_KEY_FILE = Path("license_public_key.pem")


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


def current_machine_id() -> str:
    raw = "|".join(
        [
            platform.system(),
            platform.release(),
            platform.machine(),
            platform.node(),
            hex(uuid.getnode()),
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24].upper()


def load_public_key() -> Optional[Ed25519PublicKey]:
    if not PUBLIC_KEY_FILE.exists():
        return None
    data = PUBLIC_KEY_FILE.read_bytes()
    key = serialization.load_pem_public_key(data)
    if isinstance(key, Ed25519PublicKey):
        return key
    return None


def load_saved_license_code() -> Optional[str]:
    if not LICENSE_FILE.exists():
        return None
    try:
        data = json.loads(LICENSE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return None
    code = str(data.get("license_code", "")).strip()
    return code or None


def save_license_code(code: str) -> None:
    LICENSE_FILE.write_text(
        json.dumps({"license_code": code.strip()}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def delete_saved_license() -> None:
    if LICENSE_FILE.exists():
        LICENSE_FILE.unlink()


def verify_license_code(code: str, machine_id: Optional[str] = None) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
    normalized = "".join(str(code or "").split())
    if not normalized:
        return None, "Пустой код лицензии."

    try:
        payload_b64, signature_b64 = normalized.split(".", 1)
    except ValueError:
        return None, "Неверный формат кода лицензии."

    public_key = load_public_key()
    if public_key is None:
        return None, f"Не найден публичный ключ: {PUBLIC_KEY_FILE}"

    try:
        payload_bytes = _b64url_decode(payload_b64)
        signature = _b64url_decode(signature_b64)
        public_key.verify(signature, payload_bytes)
    except (InvalidSignature, ValueError):
        return None, "Подпись лицензии недействительна."

    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except Exception:
        return None, "Поврежденные данные лицензии."

    expires_at = str(payload.get("expires_at", "")).strip()
    if not expires_at:
        return None, "В лицензии нет даты истечения."

    try:
        expires = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
    except ValueError:
        return None, "Неверная дата истечения в лицензии."

    now = datetime.now(timezone.utc)
    if expires < now:
        return None, f"Срок лицензии истек: {expires.strftime('%Y-%m-%d %H:%M UTC')}"

    expected_machine = str(payload.get("machine_id", "")).strip().upper()
    current_machine = (machine_id or current_machine_id()).upper()
    if expected_machine and expected_machine != "*" and expected_machine != current_machine:
        return None, "Лицензия выдана для другого компьютера."

    return payload, None


class LicenseDialog:
    def __init__(self, root: tk.Tk, reason: str = "") -> None:
        self.root = root
        self.result: Optional[Dict[str, str]] = None
        self.machine_id = current_machine_id()

        self.window = tk.Toplevel(root)
        self.window.title("Активация лицензии")
        self.window.geometry("760x420")
        self.window.minsize(680, 380)
        self.window.configure(bg="#0f131a")
        self.window.grab_set()
        self.window.protocol("WM_DELETE_WINDOW", self._cancel)

        frame = ttk.Frame(self.window, padding=16)
        frame.pack(fill=tk.BOTH, expand=True)

        tk.Label(
            frame,
            text="Требуется код доступа",
            bg="#0f131a",
            fg="#8fb4ff",
            font=("Consolas", 16, "bold"),
        ).pack(anchor=tk.W)

        details = reason or "Введите код лицензии. Код должен быть подписан твоим приватным ключом."
        tk.Label(
            frame,
            text=details,
            bg="#0f131a",
            fg="#d7dde8",
            justify=tk.LEFT,
            font=("Consolas", 10),
        ).pack(anchor=tk.W, pady=(8, 12))

        tk.Label(
            frame,
            text=f"Machine ID: {self.machine_id}",
            bg="#0f131a",
            fg="#ffe08a",
            font=("Consolas", 10, "bold"),
        ).pack(anchor=tk.W, pady=(0, 8))

        tk.Label(
            frame,
            text="Отправь этот Machine ID себе и сгенерируй код через license_tool.py.",
            bg="#0f131a",
            fg="#8fa1bf",
            font=("Consolas", 9),
        ).pack(anchor=tk.W, pady=(0, 8))

        self.code_text = tk.Text(
            frame,
            height=10,
            wrap=tk.WORD,
            bg="#111827",
            fg="#e6edf8",
            insertbackground="#e6edf8",
            relief=tk.FLAT,
            font=("Consolas", 10),
        )
        self.code_text.pack(fill=tk.BOTH, expand=True, pady=(0, 12))

        buttons = ttk.Frame(frame)
        buttons.pack(fill=tk.X)
        ttk.Button(buttons, text="Активировать", command=self._activate).pack(side=tk.LEFT)
        ttk.Button(buttons, text="Выход", command=self._cancel).pack(side=tk.LEFT, padx=(8, 0))

    def _activate(self) -> None:
        code = self.code_text.get("1.0", tk.END).strip()
        payload, error = verify_license_code(code, machine_id=self.machine_id)
        if error:
            messagebox.showerror("Лицензия", error, parent=self.window)
            return
        save_license_code(code)
        self.result = payload
        self.window.destroy()

    def _cancel(self) -> None:
        self.result = None
        self.window.destroy()


def ensure_valid_license(root: tk.Tk) -> Optional[Dict[str, str]]:
    machine_id = current_machine_id()
    code = load_saved_license_code()
    if code:
        payload, error = verify_license_code(code, machine_id=machine_id)
        if payload:
            return payload
        delete_saved_license()
        reason = f"{error}\n\nВведи новый код продления."
    else:
        reason = ""

    dialog = LicenseDialog(root, reason=reason)
    root.wait_window(dialog.window)
    return dialog.result


def format_license_summary(payload: Dict[str, str]) -> str:
    customer = str(payload.get("customer", "UNKNOWN")).strip()
    expires_at = str(payload.get("expires_at", "")).strip()
    machine_id = str(payload.get("machine_id", "")).strip()
    return textwrap.shorten(
        f"{customer} | до {expires_at} | {machine_id}",
        width=80,
        placeholder="...",
    )
