from __future__ import annotations

from app.schemas.conversation import ConversationIntent


ORDER_MANAGEMENT_HINTS = (
    "order status",
    "my order",
    "commande",
    "cancel my order",
    "annuler ma commande",
    "réclamation",
    "complain",
    "complaint",
    "shkaya",
    "شكاية",
    "طلب",
)

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
    "retour": (
        "retour",
        "return",
        "exchange",
        "refund",
        "remboursement",
        "replace",
        "ma3jbnich",
        "nرجعو",
        "رجعو",
        "رجع",
    ),
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
    "infos_boutique": (
        "address",
        "adresse",
        "store",
        "magasin",
        "shop",
        "phone",
        "telephone",
        "numéro",
        "numero",
        "whatsapp",
        "email",
        "contact",
        "where are you",
        "location",
        "horaire",
        "horaires",
        "open",
        "close",
        "opening",
        "closing",
        "schedule",
        "hours",
        "samedi",
        "sunday",
        "fin",
        "m7al",
        "محل",
        "السبت",
        "الأحد",
    ),
    "autre": (),
}

INTENT_PRIORITY: tuple[ConversationIntent, ...] = (
    "infos_boutique",
    "retour",
    "paiement",
    "prix",
    "livraison",
    "disponibilite",
    "infos_produit",
)


def _matches_keywords(normalized: str, keywords: tuple[str, ...]) -> bool:
    return any(keyword in normalized for keyword in keywords)


def _should_prioritize_product_availability(normalized: str) -> bool:
    availability_markers = (
        "stock",
        "disponible",
        "availability",
        "out of stock",
        "rupture",
        "kayn",
        "kayna",
        "باقي",
        "مقطوع",
    )
    product_markers = (
        "product",
        "produit",
        "speaker",
        "earbuds",
        "smartphone",
        "camera",
        "power bank",
        "jbl",
        "redmi",
        "anker",
        "amazfit",
        "tapo",
        "ugreen",
        "هاد",
    )
    return _matches_keywords(normalized, availability_markers) and _matches_keywords(
        normalized, product_markers
    )

def infer_intent_hint(message: str) -> ConversationIntent:
    normalized = message.strip().lower()
    if is_order_management_request(normalized):
        return "autre"

    if _matches_keywords(normalized, INTENT_KEYWORDS["infos_boutique"]):
        return "infos_boutique"

    if _should_prioritize_product_availability(normalized):
        return "disponibilite"

    for intent in INTENT_PRIORITY:
        if _matches_keywords(normalized, INTENT_KEYWORDS[intent]):
            return intent
    return "autre"


def source_preference(intent: ConversationIntent) -> tuple[str, ...]:
    if intent in {"livraison", "paiement", "retour"}:
        return ("faq", "business_knowledge", "product")
    if intent == "infos_boutique":
        return ("business_fact", "faq", "business_knowledge")
    if intent in {"prix", "disponibilite", "infos_produit"}:
        return ("product", "faq", "business_knowledge")
    return ("product", "faq", "business_knowledge")


def is_order_management_request(message: str) -> bool:
    normalized = message.strip().lower()
    return any(keyword in normalized for keyword in ORDER_MANAGEMENT_HINTS)


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
