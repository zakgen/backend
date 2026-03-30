from __future__ import annotations

from collections.abc import Sequence


def to_vector_literal(embedding: Sequence[float]) -> str:
    return "[" + ",".join(f"{value:.8f}" for value in embedding) + "]"
