from __future__ import annotations

import json
import os
from datetime import datetime, timezone


def save_json(path: str, data: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def update_index(path: str, items: list[str], key: str = "tickers") -> None:
    index = {
        "updatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        f"{key}Count": len(items),
        key: sorted(items),
    }
    save_json(path, index)
