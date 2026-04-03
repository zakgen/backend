from pathlib import Path

from queries.query_generator import generate_queries


def test_generate_queries_creates_balanced_multilingual_suite(tmp_path: Path) -> None:
    output_path = tmp_path / "generated_queries.json"

    queries = generate_queries(output_path=output_path)

    assert len(queries) == 30
    assert output_path.exists()
    assert sum(query.language == "english" for query in queries) == 10
    assert sum(query.language == "french" for query in queries) == 10
    assert sum(query.language == "darija" for query in queries) == 10
    assert {query.topic for query in queries} == {"products", "profile", "delivery", "orders"}
    assert any("delivery" in query.query_text.lower() for query in queries if query.language == "darija")
    assert any("Atlas Gadget Hub" in query.query_text for query in queries)

