from evaluator.config import EvalSettings
from evaluator.models import JudgeScores, QueryRecord, ScoredResult, ServiceResult
from evaluator.report_generator import ReportGenerator


def _scored_result(
    *,
    query_id: str,
    language: str,
    topic: str,
    overall_seed: int,
    passed: bool,
) -> ScoredResult:
    base_score = 5 if passed else overall_seed
    query = QueryRecord(
        id=query_id,
        language=language,
        topic=topic,
        query_text=f"Question {query_id}",
        expected_intent=f"{topic}_intent",
    )
    result = ServiceResult(
        query_id=query_id,
        query_text=query.query_text,
        language=language,
        topic=topic,
        expected_intent=query.expected_intent,
        status="success",
        latency_ms=420.0,
        ai_response="Sample response",
    )
    scores = JudgeScores(
        relevance=base_score,
        accuracy=base_score,
        language_match=base_score,
        completeness=base_score,
        tone=base_score,
        hallucination_risk=base_score,
        failure_tags=[] if passed else ["wrong_language_reply"],
        reasoning="Synthetic test score",
    )
    return ScoredResult(query=query, result=result, scores=scores)


def test_report_generator_builds_language_and_topic_breakdowns() -> None:
    settings = EvalSettings()
    report = ReportGenerator(settings).build(
        [
            _scored_result(
                query_id="en_products_001",
                language="english",
                topic="products",
                overall_seed=2,
                passed=True,
            ),
            _scored_result(
                query_id="fr_delivery_001",
                language="french",
                topic="delivery",
                overall_seed=2,
                passed=False,
            ),
            _scored_result(
                query_id="da_orders_001",
                language="darija",
                topic="orders",
                overall_seed=3,
                passed=False,
            ),
        ]
    )

    assert report.summary["total_queries"] == 3
    assert "english" in report.breakdown_by_language
    assert "delivery" in report.breakdown_by_topic
    assert report.failure_patterns[0]["pattern"] == "wrong_language_reply"
    assert report.recommendations

