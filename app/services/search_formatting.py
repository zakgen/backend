from __future__ import annotations

from typing import Any

from app.schemas.search import SearchMatch


def confidence_label(score: float) -> str:
    if score >= 0.85:
        return "high"
    if score >= 0.7:
        return "medium"
    return "low"


def format_product_match(row: dict[str, Any]) -> SearchMatch:
    score = round(float(row["score"]), 4)
    metadata = dict(row.get("metadata") or {})
    if row.get("category"):
        metadata.setdefault("category", row["category"])
    if row.get("availability"):
        metadata.setdefault("availability", row["availability"])
    metadata["confidence_label"] = confidence_label(score)

    return SearchMatch(
        type="product",
        id=row["id"],
        name=row["name"],
        description=row.get("description"),
        price=float(row["price"]) if row.get("price") is not None else None,
        currency=row.get("currency"),
        score=score,
        metadata=metadata,
    )


def format_faq_match(row: dict[str, Any]) -> SearchMatch:
    score = round(float(row["score"]), 4)
    metadata = dict(row.get("metadata") or {})
    metadata["confidence_label"] = confidence_label(score)

    return SearchMatch(
        type="faq",
        id=row["id"],
        name=row["question"],
        description=row.get("answer"),
        score=score,
        metadata=metadata,
    )


def format_business_match(row: dict[str, Any]) -> SearchMatch:
    score = round(float(row["score"]), 4)
    metadata = dict(row.get("metadata") or {})
    metadata["source_type"] = row.get("source_type", "profile")
    metadata["confidence_label"] = confidence_label(score)

    return SearchMatch(
        type="business_knowledge",
        id=row["id"],
        name=row["title"],
        description=row.get("content"),
        score=score,
        metadata=metadata,
    )
