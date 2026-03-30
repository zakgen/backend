from __future__ import annotations

import re


def normalize_phone_number(value: str | None) -> str:
    if value is None:
        return ""

    cleaned = value.strip()
    if cleaned.lower().startswith("whatsapp:"):
        cleaned = cleaned.split(":", 1)[1]

    cleaned = re.sub(r"[^\d+]", "", cleaned)
    if cleaned.startswith("00"):
        cleaned = f"+{cleaned[2:]}"
    elif cleaned and not cleaned.startswith("+"):
        cleaned = f"+{cleaned}"
    return cleaned
