from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from evaluator.config import EvalSettings
from evaluator.data_seed import EvalDataSeeder, SeededBusiness
from evaluator.report_generator import ReportGenerator
from evaluator.scorer import ResponseScorer
from evaluator.service_caller import ServiceCaller
from evaluator.utils import load_json, write_json
from queries.query_generator import generate_queries


def log_step(message: str) -> None:
    timestamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[eval] {timestamp} | {message}")


async def main() -> None:
    settings = EvalSettings()
    business_profile = load_json(settings.business_profile_path)
    seeder = EvalDataSeeder(settings)
    seeded_business: SeededBusiness | None = None

    try:
        if settings.seed_eval_data:
            log_step("Seeding temporary business data into the database.")
            seeded_business = await seeder.seed(business_profile)
            settings.business_id = seeded_business.business_id
            log_step(
                "Seeded business "
                f"'{seeded_business.name}' "
                f"(business_id={seeded_business.business_id}, "
                f"products={seeded_business.product_count}, "
                f"faqs={seeded_business.faq_count}, "
                f"delivery_zones={seeded_business.delivery_zone_count})."
            )
        else:
            log_step(
                "Skipping seed step. Using existing backend business data "
                f"for business_id={settings.business_id}."
            )

        log_step("Generating multilingual evaluation queries.")
        queries = generate_queries(
            business_profile_path=settings.business_profile_path,
            template_path=settings.query_templates_path,
            output_path=settings.generated_queries_path,
        )
        log_step(f"Generated {len(queries)} queries at {settings.generated_queries_path}.")

        log_step(
            f"Calling backend reply service at {settings.normalized_base_url} "
            f"for business_id={settings.business_id}."
        )
        service_results = await ServiceCaller(settings).run(queries)
        write_json(settings.raw_results_path, [item.model_dump() for item in service_results])
        success_count = sum(1 for item in service_results if item.status == "success")
        failure_count = len(service_results) - success_count
        log_step(
            f"Backend calls completed: {success_count} successful, {failure_count} failed. "
            f"Raw results written to {settings.raw_results_path}."
        )

        log_step(f"Scoring responses with judge model '{settings.judge_model}'.")
        scored_results = await ResponseScorer(settings).run(
            queries=queries,
            results=service_results,
            business_profile=business_profile,
        )
        write_json(settings.scored_results_path, [item.model_dump() for item in scored_results])
        passed_count = sum(1 for item in scored_results if item.scores.passed)
        log_step(
            f"Scoring completed: {passed_count}/{len(scored_results)} passed. "
            f"Scored results written to {settings.scored_results_path}."
        )

        log_step("Building final evaluation reports.")
        report_generator = ReportGenerator(settings)
        report = report_generator.build(scored_results)
        report_generator.write(report)

        log_step(f"JSON report written to {settings.report_json_path}.")
        log_step(f"Markdown report written to {settings.report_md_path}.")
        log_step(f"Pass rate: {report.summary['pass_rate']}%.")
        log_step(f"Average overall score: {report.summary['average_overall_score']}/5.")
    finally:
        if settings.seed_eval_data and settings.cleanup_seed_data and seeded_business is not None:
            log_step(f"Cleaning up seeded business_id={seeded_business.business_id}.")
            await seeder.cleanup(seeded_business.business_id)
            log_step("Seed cleanup completed.")


if __name__ == "__main__":
    asyncio.run(main())
