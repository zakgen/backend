from __future__ import annotations

def normalize_language_label(language: str | None, fallback: str = "english") -> str:
    normalized = (language or "").strip().lower()
    if normalized in {"darija", "moroccan arabic", "ma", "ary"}:
        return "darija"
    if normalized in {"ar", "arabic"}:
        return "darija"
    if normalized in {"fr", "french", "français", "francais"}:
        return "french"
    if normalized in {"en", "english"}:
        return "english"
    return fallback
