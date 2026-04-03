from __future__ import annotations

from abc import ABC, abstractmethod

from app.schemas.ai import AIModelReply
from app.schemas.order_confirmation import OrderSessionInterpretation


class AbstractLLMProvider(ABC):
    provider_name: str
    model_name: str

    @abstractmethod
    async def generate_structured_reply(
        self, *, system_prompt: str, user_prompt: str
    ) -> tuple[AIModelReply, dict]:
        raise NotImplementedError

    @abstractmethod
    async def detect_language(self, *, message: str) -> tuple[str, dict]:
        raise NotImplementedError

    @abstractmethod
    async def interpret_order_session(
        self,
        *,
        customer_message: str,
        preferred_language: str | None,
        session_status: str,
        order_snapshot: dict,
    ) -> tuple[OrderSessionInterpretation, dict]:
        raise NotImplementedError
