from __future__ import annotations

from typing import Any

from app.schemas.business import BusinessProfile


PROMPT_VERSION = "zakbot-grounded-reply-v1"


def _safe_join(values: list[str]) -> str:
    return ", ".join(item for item in values if item)


def _format_history(recent_messages: list[dict[str, str]]) -> str:
    if not recent_messages:
        return "No prior conversation context."
    return "\n".join(
        f"- {item['direction']}: {item['text']}" for item in recent_messages if item.get("text")
    )


def _format_source(item: dict[str, Any]) -> str:
    score = round(float(item.get("score") or 0), 4)
    line = f"- [{item['type']}:{item['id']}] {item['name']} (score={score})"
    content = str(item.get("content") or "").strip()
    if content:
        line += f": {content}"
    metadata = item.get("metadata") or {}
    if metadata:
        metadata_line = ", ".join(
            f"{key}={value}" for key, value in metadata.items() if value not in (None, "", [], {})
        )
        if metadata_line:
            line += f" | metadata: {metadata_line}"
    return line


def build_ai_reply_prompts(
    *,
    business_profile: BusinessProfile,
    customer_message: str,
    recent_messages: list[dict[str, str]],
    selected_sources: list[dict[str, Any]],
    language_hint: str,
    intent_hint: str,
) -> tuple[str, str]:
    store_configuration = [
        f"Business name: {business_profile.name}",
        f"Summary: {business_profile.summary}",
        f"Niche: {business_profile.niche}",
        f"Tone of voice: {business_profile.tone_of_voice}",
        f"Supported languages: {_safe_join(business_profile.supported_languages)}",
    ]

    evidence_block = (
        "\n".join(_format_source(item) for item in selected_sources)
        if selected_sources
        else "No retrieved business evidence was found."
    )

    system_prompt = f"""
You are ZakBot, a Moroccan ecommerce WhatsApp assistant.
You must answer ONLY from the merchant data provided.
Never invent delivery zones, prices, stock, return policy, payment methods, or product facts.
If evidence is weak or missing, set needs_human=true or ask a short clarifying question.
Mirror the customer's language/register when possible while keeping the store tone.
Keep answers concise and WhatsApp-friendly.

Return valid JSON with exactly these fields:
- reply_text: string or null
- intent: one of livraison, prix, disponibilite, retour, paiement, infos_produit, autre
- language: short string
- used_sources: array of objects with type, id, name, score, metadata
- grounded: boolean
- needs_human: boolean
- confidence: number between 0 and 1
- reason_code: short string
- follow_up_question: string or null

Prompt version: {PROMPT_VERSION}
""".strip()

    user_prompt = f"""
Customer message:
{customer_message}

Language hint:
{language_hint}

Intent hint:
{intent_hint}

Store configuration:
{chr(10).join(f"- {item}" for item in store_configuration if item.split(': ', 1)[1].strip())}

Recent conversation:
{_format_history(recent_messages)}

Retrieved evidence:
{evidence_block}

Instructions:
- If you make a factual claim, cite at least one used source from the retrieved evidence.
- used_sources must reuse exact source entries from the retrieved evidence block, including the same type and id.
- Allowed source types are product, faq, business_knowledge, and business_fact.
- If the user asks about unsupported or missing merchant information, do not guess.
- If a clarifying question is the safest reply, put it in reply_text and optionally repeat it in follow_up_question.
- If human intervention is needed, set needs_human=true.
""".strip()

    return system_prompt, user_prompt
