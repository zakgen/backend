from __future__ import annotations

import base64
import hashlib
import json
from typing import Any

from cryptography.fernet import Fernet, InvalidToken
from fastapi import HTTPException, status

from app.config import Settings, get_settings


class AppCryptoService:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    def _build_fernet(self) -> Fernet:
        secret = self.settings.app_encryption_key
        if secret is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="APP_ENCRYPTION_KEY is not configured.",
            )
        digest = hashlib.sha256(secret.get_secret_value().encode("utf-8")).digest()
        return Fernet(base64.urlsafe_b64encode(digest))

    def encrypt_text(self, value: str) -> str:
        return self._build_fernet().encrypt(value.encode("utf-8")).decode("utf-8")

    def decrypt_text(self, value: str, *, ttl_seconds: int | None = None) -> str:
        try:
            decrypted = self._build_fernet().decrypt(
                value.encode("utf-8"),
                ttl=ttl_seconds,
            )
        except InvalidToken as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid or expired encrypted payload.",
            ) from exc
        return decrypted.decode("utf-8")

    def encrypt_json(self, payload: dict[str, Any]) -> str:
        return self.encrypt_text(json.dumps(payload, separators=(",", ":"), sort_keys=True))

    def decrypt_json(
        self, value: str, *, ttl_seconds: int | None = None
    ) -> dict[str, Any]:
        raw = self.decrypt_text(value, ttl_seconds=ttl_seconds)
        decoded = json.loads(raw)
        if not isinstance(decoded, dict):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Encrypted payload did not decode to an object.",
            )
        return decoded
