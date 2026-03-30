from __future__ import annotations

from abc import ABC, abstractmethod

from app.schemas.ai import AIModelReply


class AbstractLLMProvider(ABC):
    provider_name: str
    model_name: str

    @abstractmethod
    async def generate_structured_reply(
        self, *, system_prompt: str, user_prompt: str
    ) -> tuple[AIModelReply, dict]:
        raise NotImplementedError
