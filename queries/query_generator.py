from __future__ import annotations

from collections import Counter
from pathlib import Path

from evaluator.models import QueryRecord
from evaluator.utils import load_json, write_json


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROFILE_PATH = ROOT / "data" / "business_profile.json"
DEFAULT_TEMPLATE_PATH = ROOT / "data" / "query_templates.json"
DEFAULT_OUTPUT_PATH = ROOT / "queries" / "generated_queries.json"


def _build_placeholder_context(profile: dict) -> dict[str, str]:
    products = profile["products"]
    available = [product for product in products if product["availability"] == "in_stock"]
    unavailable = [product for product in products if product["availability"] != "in_stock"]
    categories = sorted({product["category"] for product in products})
    zones = profile["delivery"]["zones"]
    business = profile["business"]

    return {
        "business_name": business["name"],
        "city": business["location"]["city"],
        "business_phone": business["contact"]["phone"],
        "return_days": str(profile["return_policy"]["window_days"]),
        "available_product_1": available[0]["name"],
        "available_product_2": available[1]["name"],
        "available_product_3": available[2]["name"],
        "unavailable_product_1": unavailable[0]["name"],
        "category_1": categories[0],
        "category_2": categories[1],
        "delivery_zone_1": zones[0]["city"],
        "delivery_zone_2": zones[1]["city"],
        "delivery_zone_3": zones[3]["city"],
    }


def generate_queries(
    *,
    business_profile_path: Path = DEFAULT_PROFILE_PATH,
    template_path: Path = DEFAULT_TEMPLATE_PATH,
    output_path: Path = DEFAULT_OUTPUT_PATH,
) -> list[QueryRecord]:
    profile = load_json(business_profile_path)
    templates = load_json(template_path)
    context = _build_placeholder_context(profile)

    generated: list[QueryRecord] = []
    for language, language_templates in templates.items():
        for index, template in enumerate(language_templates, start=1):
            generated.append(
                QueryRecord(
                    id=f"{language[:2]}_{template['topic']}_{index:03d}",
                    language=language,
                    topic=template["topic"],
                    query_text=template["query_text"].format(**context),
                    expected_intent=template["expected_intent"],
                    difficulty=template.get("difficulty", "medium"),
                    edge_case_tags=template.get("edge_case_tags", []),
                )
            )

    _validate_distribution(generated)
    write_json(output_path, [query.model_dump() for query in generated])
    return generated


def _validate_distribution(queries: list[QueryRecord]) -> None:
    if len(queries) < 30:
        raise ValueError("Expected at least 30 generated queries.")

    language_counts = Counter(query.language for query in queries)
    for language in ("english", "french", "darija"):
        if language_counts[language] < 10:
            raise ValueError(f"Expected at least 10 queries for {language}.")

    topic_counts = Counter(query.topic for query in queries)
    for topic in ("products", "profile", "delivery", "orders"):
        if topic_counts[topic] == 0:
            raise ValueError(f"Expected at least one query for topic {topic}.")


if __name__ == "__main__":
    generate_queries()
