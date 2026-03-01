import argparse
import base64
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from license_manager import PUBLIC_KEY_FILE, current_machine_id


PRIVATE_KEY_FILE = Path("license_private_key.pem")


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def init_keys(force: bool) -> None:
    if (PRIVATE_KEY_FILE.exists() or PUBLIC_KEY_FILE.exists()) and not force:
        raise SystemExit("Ключи уже существуют. Используй --force для перезаписи.")

    private_key = Ed25519PrivateKey.generate()
    public_key = private_key.public_key()

    PRIVATE_KEY_FILE.write_bytes(
        private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    PUBLIC_KEY_FILE.write_bytes(
        public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
    )
    print(f"Private key: {PRIVATE_KEY_FILE}")
    print(f"Public key:  {PUBLIC_KEY_FILE}")
    print("Держи private key только у себя. Его нельзя отдавать вместе с exe.")


def load_private_key() -> Ed25519PrivateKey:
    if not PRIVATE_KEY_FILE.exists():
        raise SystemExit(f"Не найден {PRIVATE_KEY_FILE}. Сначала выполни: python license_tool.py init-keys")
    return serialization.load_pem_private_key(
        PRIVATE_KEY_FILE.read_bytes(),
        password=None,
    )


def issue_license(customer: str, machine_id: str, days: int, expires: str | None) -> None:
    private_key = load_private_key()
    now = datetime.now(timezone.utc)
    if expires:
        expires_at = datetime.fromisoformat(expires).replace(tzinfo=timezone.utc)
    else:
        expires_at = now + timedelta(days=days)

    payload = {
        "version": 1,
        "customer": customer.strip() or "UNKNOWN",
        "machine_id": machine_id.strip().upper() or "*",
        "issued_at": now.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "expires_at": expires_at.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }
    payload_bytes = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    signature = private_key.sign(payload_bytes)
    code = f"{_b64url_encode(payload_bytes)}.{_b64url_encode(signature)}"

    print("Machine ID:", payload["machine_id"])
    print("Expires:   ", payload["expires_at"])
    print("Customer:  ", payload["customer"])
    print()
    print(code)


def main() -> None:
    parser = argparse.ArgumentParser(description="License utility for Crypto Arbitrage IDE")
    sub = parser.add_subparsers(dest="cmd", required=True)

    init_cmd = sub.add_parser("init-keys", help="Generate private/public key pair")
    init_cmd.add_argument("--force", action="store_true", help="Overwrite existing keys")

    sub.add_parser("machine", help="Print current machine id")

    issue_cmd = sub.add_parser("issue", help="Issue a signed license code")
    issue_cmd.add_argument("--customer", required=True, help="Customer label")
    issue_cmd.add_argument("--machine", required=True, help="Machine ID from the target PC, or *")
    issue_cmd.add_argument("--days", type=int, default=30, help="License duration in days")
    issue_cmd.add_argument("--expires", default=None, help="Absolute UTC date/time in ISO format")

    args = parser.parse_args()
    if args.cmd == "init-keys":
        init_keys(force=args.force)
        return
    if args.cmd == "machine":
        print(current_machine_id())
        return
    if args.cmd == "issue":
        issue_license(
            customer=args.customer,
            machine_id=args.machine,
            days=args.days,
            expires=args.expires,
        )


if __name__ == "__main__":
    main()
