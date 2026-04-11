from app.services.ai_helpers import normalize_language_label


def test_normalize_language_label_maps_supported_variants() -> None:
    assert normalize_language_label("ar") == "darija"
    assert normalize_language_label("french") == "french"
    assert normalize_language_label("en") == "english"
