from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

from evaluator.config import EvalSettings
from evaluator.models import AggregateBreakdown, DIMENSION_FIELDS, EvalReport, ScoredResult
from evaluator.utils import utc_now_iso, write_json


class ReportGenerator:
    def __init__(self, settings: EvalSettings) -> None:
        self.settings = settings

    def build(self, scored_results: list[ScoredResult]) -> EvalReport:
        summary = self._build_summary(scored_results)
        by_language = self._build_breakdown(scored_results, key="language")
        by_topic = self._build_breakdown(scored_results, key="topic")
        failure_patterns = self._build_failure_patterns(scored_results)
        recommendations = self._build_recommendations(summary, failure_patterns)

        sorted_results = sorted(
            scored_results,
            key=lambda item: (item.scores.overall_score, item.result.latency_ms),
            reverse=True,
        )
        best_responses = [self._result_snapshot(item) for item in sorted_results[:3]]
        worst_responses = [self._result_snapshot(item) for item in sorted_results[-3:]]

        return EvalReport(
            generated_at=utc_now_iso(),
            config={
                "base_url": self.settings.normalized_base_url,
                "business_id": self.settings.business_id,
                "judge_model": self.settings.judge_model,
                "max_concurrency": self.settings.max_concurrency,
            },
            summary=summary,
            breakdown_by_language=by_language,
            breakdown_by_topic=by_topic,
            best_responses=best_responses,
            worst_responses=worst_responses,
            failure_patterns=failure_patterns,
            recommendations=recommendations,
            results=scored_results,
        )

    def write(self, report: EvalReport) -> None:
        write_json(self.settings.report_json_path, report.model_dump())
        self.settings.report_md_path.parent.mkdir(parents=True, exist_ok=True)
        self.settings.report_md_path.write_text(self._to_markdown(report), encoding="utf-8")

    def _build_summary(self, scored_results: list[ScoredResult]) -> dict[str, Any]:
        count = len(scored_results)
        passed = sum(1 for item in scored_results if item.scores.passed)
        average_scores = {
            field: round(sum(getattr(item.scores, field) for item in scored_results) / count, 2)
            for field in DIMENSION_FIELDS
        }
        average_latency = round(
            sum(item.result.latency_ms for item in scored_results) / count, 2
        )
        average_overall = round(
            sum(item.scores.overall_score for item in scored_results) / count, 2
        )
        return {
            "total_queries": count,
            "pass_rate": round((passed / count) * 100, 2),
            "average_overall_score": average_overall,
            "average_latency_ms": average_latency,
            "average_scores": average_scores,
        }

    def _build_breakdown(
        self, scored_results: list[ScoredResult], *, key: str
    ) -> dict[str, AggregateBreakdown]:
        grouped: dict[str, list[ScoredResult]] = defaultdict(list)
        for item in scored_results:
            grouped[getattr(item.query, key)].append(item)

        breakdown: dict[str, AggregateBreakdown] = {}
        for name, items in grouped.items():
            count = len(items)
            breakdown[name] = AggregateBreakdown(
                count=count,
                pass_rate=round(sum(1 for item in items if item.scores.passed) / count * 100, 2),
                average_overall_score=round(
                    sum(item.scores.overall_score for item in items) / count, 2
                ),
                average_latency_ms=round(
                    sum(item.result.latency_ms for item in items) / count, 2
                ),
                dimension_averages={
                    field: round(sum(getattr(item.scores, field) for item in items) / count, 2)
                    for field in DIMENSION_FIELDS
                },
            )
        return breakdown

    def _build_failure_patterns(self, scored_results: list[ScoredResult]) -> list[dict[str, Any]]:
        counter: Counter[str] = Counter()
        for item in scored_results:
            counter.update(item.scores.failure_tags)
        return [
            {"pattern": tag, "count": count}
            for tag, count in counter.most_common()
        ]

    def _build_recommendations(
        self,
        summary: dict[str, Any],
        failure_patterns: list[dict[str, Any]],
    ) -> list[str]:
        recommendations: list[str] = []
        scores = summary["average_scores"]
        if scores["accuracy"] < 4:
            recommendations.append(
                "Strengthen grounding rules and retrieval checks so answers do not contradict the business profile."
            )
        if scores["language_match"] < 4:
            recommendations.append(
                "Add explicit language control in the reply prompt and validate language before sending the answer."
            )
        if scores["completeness"] < 4:
            recommendations.append(
                "Improve multi-intent handling so the assistant answers every part of compound customer questions."
            )

        top_patterns = {item["pattern"] for item in failure_patterns[:5]}
        if "invalid_response" in top_patterns or "http_error" in top_patterns or "timeout" in top_patterns:
            recommendations.append(
                "Harden the reply endpoint with better timeout budgets, retries, and response-shape validation."
            )
        if any(pattern.endswith("hallucination") for pattern in top_patterns):
            recommendations.append(
                "Reduce hallucinations by forcing unsupported requests to escalate instead of improvising."
            )
        if not recommendations:
            recommendations.append(
                "The system is stable overall; focus next on raising answer richness while keeping the same grounding quality."
            )
        return recommendations

    def _result_snapshot(self, item: ScoredResult) -> dict[str, Any]:
        return {
            "query_id": item.query.id,
            "language": item.query.language,
            "topic": item.query.topic,
            "query_text": item.query.query_text,
            "ai_response": item.result.ai_response,
            "overall_score": item.scores.overall_score,
            "passed": item.scores.passed,
            "failure_tags": item.scores.failure_tags,
        }

    def _to_markdown(self, report: EvalReport) -> str:
        summary = report.summary
        lines = [
            "# AI Reply Evaluation Report",
            "",
            "## Run Summary",
            f"- Generated at: {report.generated_at}",
            f"- Backend endpoint: {report.config['base_url']}",
            f"- Business ID: {report.config['business_id']}",
            f"- Judge model: {report.config['judge_model']}",
            f"- Total queries: {summary['total_queries']}",
            f"- Pass rate: {summary['pass_rate']}%",
            f"- Average overall score: {summary['average_overall_score']}/5",
            f"- Average latency: {summary['average_latency_ms']} ms",
            "",
            "## Average Scores",
        ]

        for field, score in summary["average_scores"].items():
            lines.append(f"- {field.replace('_', ' ').title()}: {score}/5")

        lines.extend(["", "## Breakdown by Language"])
        for language, breakdown in report.breakdown_by_language.items():
            lines.append(
                f"- {language.title()}: pass rate {breakdown.pass_rate}%, "
                f"average score {breakdown.average_overall_score}/5"
            )

        lines.extend(["", "## Breakdown by Topic"])
        for topic, breakdown in report.breakdown_by_topic.items():
            lines.append(
                f"- {topic.title()}: pass rate {breakdown.pass_rate}%, "
                f"average score {breakdown.average_overall_score}/5"
            )

        lines.extend(["", "## Top 3 Best Responses"])
        for item in report.best_responses:
            lines.append(
                f"- [{item['query_id']}] {item['overall_score']}/5 | {item['query_text']} -> {item['ai_response']}"
            )

        lines.extend(["", "## Top 3 Worst Responses"])
        for item in report.worst_responses:
            lines.append(
                f"- [{item['query_id']}] {item['overall_score']}/5 | {item['query_text']} -> {item['ai_response']}"
            )

        lines.extend(["", "## Failure Patterns"])
        if report.failure_patterns:
            for item in report.failure_patterns:
                lines.append(f"- {item['pattern']}: {item['count']}")
        else:
            lines.append("- No recurring failure patterns detected.")

        lines.extend(["", "## Recommendations"])
        for recommendation in report.recommendations:
            lines.append(f"- {recommendation}")

        return "\n".join(lines) + "\n"
