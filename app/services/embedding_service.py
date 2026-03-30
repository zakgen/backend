from __future__ import annotations

from collections.abc import Sequence

from openai import AsyncOpenAI

from app.config import Settings, get_settings


class EmbeddingService:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self._client: AsyncOpenAI | None = None

    async def embed_text(self, text: str) -> list[float]:
        embeddings = await self.embed_texts([text])
        return embeddings[0]

    async def embed_texts(self, texts: Sequence[str]) -> list[list[float]]:
        cleaned = [text.strip() for text in texts if text and text.strip()]
        if not cleaned:
            return []

        if self.settings.openai_api_key is None:
            raise RuntimeError("OPENAI_API_KEY is required to generate embeddings.")

        if self._client is None:
            self._client = AsyncOpenAI(
                api_key=self.settings.openai_api_key.get_secret_value()
            )

        embeddings: list[list[float]] = []
        batch_size = 50
        for start in range(0, len(cleaned), batch_size):
            batch = cleaned[start : start + batch_size]
            response = await self._client.embeddings.create(
                model=self.settings.embedding_model,
                input=batch,
            )
            for item in sorted(response.data, key=lambda entry: entry.index):
                embeddings.append(item.embedding)

        return embeddings
