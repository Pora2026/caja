import json
import os
from datetime import UTC, date, datetime
from typing import Any, Dict, Optional, Tuple

import requests


def get_backup_dir(base_dir: str) -> str:
    backup_dir = os.path.join(base_dir, "Backup")
    os.makedirs(backup_dir, exist_ok=True)
    return backup_dir


def write_backup_payload(payload: Dict[str, Any], backup_dir: str, prefix: str = "caja") -> Tuple[Optional[str], Optional[str]]:
    try:
        today = date.today().isoformat()
        latest_path = os.path.join(backup_dir, f"{prefix}_daily_latest.json")
        dated_path = os.path.join(backup_dir, f"{prefix}_{today}.json")

        raw = json.dumps(payload, ensure_ascii=False, indent=2)

        with open(latest_path, "w", encoding="utf-8") as f:
            f.write(raw)

        with open(dated_path, "w", encoding="utf-8") as f:
            f.write(raw)

        return latest_path, dated_path
    except Exception as e:
        print("Backup write error:", e)
        return None, None


def upload_backup_via_webhook(file_path: str, webhook_url: str, destination_name: Optional[str] = None) -> Optional[dict]:
    if not webhook_url:
        return None

    with open(file_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    body = {
        "filename": destination_name or os.path.basename(file_path),
        "payload": payload,
        "sent_at": datetime.now(UTC).isoformat(),
    }

    response = requests.post(webhook_url, json=body, timeout=30)
    response.raise_for_status()
    data = response.json()
    if data.get("ok"):
        print("Backup enviado a Drive:", body["filename"])
    else:
        print("Webhook backup error:", data)
    return data


def perform_backup(
    payload: Dict[str, Any],
    base_dir: str,
    webhook_url: Optional[str] = None,
    prefix: str = "caja",
) -> Tuple[Optional[str], Optional[str]]:
    backup_dir = get_backup_dir(base_dir)
    latest_path, dated_path = write_backup_payload(payload, backup_dir, prefix=prefix)

    if latest_path and webhook_url:
        try:
            upload_backup_via_webhook(latest_path, webhook_url, f"{prefix}_daily_latest.json")
        except Exception as ex:
            print(f"No se pudo subir latest por webhook: {ex}")

    if dated_path and webhook_url:
        try:
            upload_backup_via_webhook(dated_path, webhook_url, os.path.basename(dated_path))
        except Exception as ex:
            print(f"No se pudo subir diario por webhook: {ex}")

    return latest_path, dated_path
