from __future__ import annotations

import re

from app.schemas.conversation import ConversationIntent


ARABIC_CHAR_RE = re.compile(r"[\u0600-\u06FF]")
FRENCH_HINTS = {
    "bonjour",
    "livraison",
    "retour",
    "paiement",
    "prix",
    "disponible",
    "combien",
    "merci",
}
DARIJA_HINTS = {
    "wach",
    "kayn",
    "kayna",
    "kifach",
    "bghit",
    "salam",
    "3afak",
    "fin",
    "chhal",
    "l",
}

INTENT_KEYWORDS: dict[ConversationIntent, tuple[str, ...]] = {
    "livraison": (
        "livraison",
        "delivery",
        "ship",
        "shipping",
        "rabat",
        "casa",
        "casablanca",
        "sale",
        "salé",
        "fes",
        "fès",
        "agadir",
        "kayn livraison",
        "tawsil",
    ),
    "prix": ("prix", "price", "cost", "chhal", "combien"),
    "disponibilite": (
        "disponible",
        "availability",
        "stock",
        "rupture",
        "in stock",
        "out of stock",
        "kayn",
        "kayna",
    ),
    "retour": ("retour", "return", "exchange", "refund", "remboursement", "replace"),
    "paiement": (
        "paiement",
        "payment",
        "cash",
        "cod",
        "bank transfer",
        "carte",
        "card",
    ),
    "infos_produit": (
        "taille",
        "size",
        "couleur",
        "color",
        "matiere",
        "material",
        "produit",
        "product",
        "variant",
    ),
    "autre": (),
}


def detect_language_hint(message: str) -> str:
    text = message.strip().lower()
    if not text:
        return "customer_language"
    if ARABIC_CHAR_RE.search(text):
        return "arabic"

    tokens = set(re.findall(r"[a-zA-ZÀ-ÿ']+", text))
    if tokens & DARIJA_HINTS:
        return "darija"
    if tokens & FRENCH_HINTS or any(char in text for char in "éèàùç"):
        return "french"
    return "customer_language"


def infer_intent_hint(message: str) -> ConversationIntent:
    normalized = message.strip().lower()
    for intent, keywords in INTENT_KEYWORDS.items():
        if intent == "autre":
            continue
        if any(keyword in normalized for keyword in keywords):
            return intent
    return "autre"


def source_preference(intent: ConversationIntent) -> tuple[str, ...]:
    if intent in {"livraison", "paiement", "retour"}:
        return ("faq", "business_knowledge", "product")
    if intent in {"prix", "disponibilite", "infos_produit"}:
        return ("product", "faq", "business_knowledge")
    return ("product", "faq", "business_knowledge")
