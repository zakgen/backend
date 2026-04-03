from evaluator.config import EvalSettings
from evaluator.data_seed import EvalDataSeeder
from evaluator.utils import load_json


def test_data_seed_builders_cover_business_products_and_faqs() -> None:
    settings = EvalSettings()
    seeder = EvalDataSeeder(settings)
    profile = load_json(settings.business_profile_path)

    upsert_request = seeder._build_business_upsert_request(profile)
    dashboard_payload = seeder._build_dashboard_business_payload(profile)
    product_items = seeder._build_product_items(profile)
    faq_items = seeder._build_faq_requests(99, profile)

    assert upsert_request.name == profile["business"]["name"]
    assert upsert_request.city == profile["business"]["location"]["city"]
    assert dashboard_payload["profile_metadata"]["tracking_available"] is True
    assert dashboard_payload["profile_metadata"]["support_phone"] == profile["business"]["contact"]["phone"]
    assert len(product_items) == len(profile["products"])
    assert len(faq_items) >= len(profile["delivery"]["zones"]) + 2
    assert any(faq.external_id == "delivery-tracking" for faq in faq_items)
