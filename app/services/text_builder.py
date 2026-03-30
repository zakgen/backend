from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def _join_list(values: list[Any] | None) -> str | None:
    if not values:
        return None
    cleaned: list[str] = []
    for value in values:
        if isinstance(value, dict):
            name = str(value.get("name") or "").strip()
            if not name:
                continue
            extra = value.get("additional_price")
            cleaned.append(f"{name} (+{extra})" if extra is not None else name)
            continue
        string_value = str(value).strip()
        if string_value:
            cleaned.append(string_value)
    if not cleaned:
        return None
    return ", ".join(cleaned)


def _append(parts: list[str], label: str, value: Any) -> None:
    if value is None:
        return
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            suffix = "" if stripped.endswith((".", "?", "!")) else "."
            parts.append(f"{label}: {stripped}{suffix}")
        return
    parts.append(f"{label}: {value}.")


def build_product_embedding_text(product: Mapping[str, Any]) -> str:
    parts: list[str] = []
    _append(parts, "Product", product.get("name"))
    _append(parts, "Description", product.get("description"))
    _append(parts, "Category", product.get("category"))

    price = product.get("price")
    currency = product.get("currency")
    if price is not None:
        parts.append(f"Price: {price} {currency or 'MAD'}.")

    _append(parts, "Availability", product.get("availability"))
    _append(parts, "Variants", _join_list(product.get("variants")))
    _append(parts, "Tags", _join_list(product.get("tags")))

    metadata = product.get("metadata") or {}
    if metadata:
        flat_metadata = ", ".join(
            f"{key}={value}" for key, value in metadata.items() if value is not None
        )
        if flat_metadata:
            parts.append(f"Metadata: {flat_metadata}.")

    return " ".join(parts).strip()


def build_business_profile_text(business: Mapping[str, Any]) -> str:
    parts: list[str] = []
    _append(parts, "Business", business.get("name"))
    _append(parts, "Description", business.get("description"))
    _append(parts, "City", business.get("city"))
    _append(parts, "Shipping policy", business.get("shipping_policy"))
    _append(parts, "Delivery zones", _join_list(business.get("delivery_zones")))
    _append(parts, "Payment methods", _join_list(business.get("payment_methods")))

    profile_metadata = business.get("profile_metadata") or {}
    if profile_metadata:
        flat_metadata = ", ".join(
            f"{key}={value}" for key, value in profile_metadata.items() if value is not None
        )
        if flat_metadata:
            parts.append(f"Business metadata: {flat_metadata}.")

    return " ".join(parts).strip()


def build_faq_embedding_text(faq: Mapping[str, Any]) -> str:
    parts: list[str] = []
    _append(parts, "FAQ question", faq.get("question"))
    _append(parts, "FAQ answer", faq.get("answer"))

    metadata = faq.get("metadata") or {}
    if metadata:
        flat_metadata = ", ".join(
            f"{key}={value}" for key, value in metadata.items() if value is not None
        )
        if flat_metadata:
            parts.append(f"Metadata: {flat_metadata}.")

    return " ".join(parts).strip()
