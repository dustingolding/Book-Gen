import argparse
import json
import os
from datetime import date

from app.db import initialize_db
from app.logging import configure_logging
from app.config import get_settings
from app.service_endpoints import resolve_service_uri


def _default_run_date() -> str:
    return date.today().isoformat()


def _apply_local_service_overrides() -> None:
    cfg = get_settings()
    resolved_prefect = resolve_service_uri(cfg.prefect_api_url, cfg.prefect_local_api_url)
    if resolved_prefect and resolved_prefect != cfg.prefect_api_url:
        os.environ["PREFECT_API_URL"] = resolved_prefect


def main() -> None:
    parser = argparse.ArgumentParser(prog="slw-dailycast")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init-db")

    p_flow = sub.add_parser("run-flow")
    p_flow.add_argument("--run-date", default=_default_run_date())
    p_editorial = sub.add_parser("run-editorial-flow")
    p_editorial.add_argument("--run-date", default=_default_run_date())
    p_flow_v4 = sub.add_parser("run-flow-v4")
    p_flow_v4.add_argument("--run-date", default=_default_run_date())
    p_flow_legacy = sub.add_parser("run-flow-legacy")
    p_flow_legacy.add_argument("--run-date", default=_default_run_date())

    p_book_flow = sub.add_parser("run-book-flow")
    p_book_flow.add_argument("--project-id", required=True)
    p_book_flow.add_argument("--run-date", default=_default_run_date())
    p_book_flow.add_argument("--bookspec-key", default=None)
    p_book_flow.add_argument("--bookspec-path", default=None)

    p_book = sub.add_parser("bookgen")
    p_book.add_argument("--project-id", required=True)
    p_book.add_argument("--run-date", default=_default_run_date())
    p_book.add_argument("--bookspec-key", default=None)
    p_book.add_argument("--bookspec-path", default=None)

    p_book_inspect = sub.add_parser("bookgen-inspect")
    p_book_inspect.add_argument("--project-id", required=True)
    p_book_inspect.add_argument("--chapter-index", required=True, type=int)
    p_book_inspect.add_argument("--installment-id", default=None)

    p_book_generation = sub.add_parser("bookgen-generation-summary")
    p_book_generation.add_argument("--project-id", required=True)
    p_book_generation.add_argument("--installment-id", default=None)

    p_book_approve = sub.add_parser("bookgen-approve")
    p_book_approve.add_argument("--project-id", required=True)
    p_book_approve.add_argument("--installment-id", default=None)
    p_book_approve.add_argument("--decision", required=True)
    p_book_approve.add_argument("--note", default="")

    p_book_report = sub.add_parser("bookgen-report")
    p_book_report.add_argument("--project-id", required=True)
    p_book_report.add_argument("--installment-id", default=None)

    for cmd in [
        "editorial-build",
        "ingest-sports",
        "ingest-news",
        "normalize",
        "rank",
        "factpack",
        "verify-factpack",
        "quality-gate",
        "render",
        "publish",
    ]:
        p = sub.add_parser(cmd)
        p.add_argument("--run-date", default=_default_run_date())

    args = parser.parse_args()
    _apply_local_service_overrides()
    configure_logging()

    if args.cmd == "init-db":
        initialize_db()
        return
    if args.cmd == "run-flow":
        from app.flows.dailycast_flow_v4 import dailycast_parent_flow_v4

        dailycast_parent_flow_v4(run_date=args.run_date)
        return
    if args.cmd == "run-editorial-flow":
        from app.flows.editorial_flow import editorial_parent_flow

        editorial_parent_flow(run_date=args.run_date)
        return
    if args.cmd == "run-flow-v4":
        from app.flows.dailycast_flow_v4 import dailycast_parent_flow_v4

        dailycast_parent_flow_v4(run_date=args.run_date)
        return
    if args.cmd == "run-flow-legacy":
        from app.flows.dailycast_flow import dailycast_parent_flow

        dailycast_parent_flow(run_date=args.run_date)
        return
    if args.cmd == "run-book-flow":
        from app.flows.bookgen_flow import bookgen_parent_flow

        bookgen_parent_flow(
            project_id=args.project_id,
            run_date=args.run_date,
            bookspec_key=args.bookspec_key,
            bookspec_path=args.bookspec_path,
        )
        return

    from app.services import (
        bookgen,
        editorial_engine,
        factpack,
        ingest_news,
        ingest_sports,
        normalize,
        publish,
        quality_gate,
        rank,
        render_markdown,
        verify_factpack,
    )

    dispatch = {
        "editorial-build": editorial_engine.run,
        "ingest-sports": ingest_sports.run,
        "ingest-news": ingest_news.run,
        "normalize": normalize.run,
        "rank": rank.run,
        "factpack": factpack.run,
        "verify-factpack": verify_factpack.run,
        "quality-gate": quality_gate.run,
        "render": render_markdown.run,
        "publish": publish.run,
    }
    if args.cmd == "bookgen":
        bookgen.run(
            project_id=args.project_id,
            run_date=args.run_date,
            bookspec_key=args.bookspec_key,
            bookspec_path=args.bookspec_path,
        )
        return
    if args.cmd == "bookgen-inspect":
        print(
            json.dumps(
                bookgen.inspect_chapter(
                    project_id=args.project_id,
                    chapter_index=args.chapter_index,
                    installment_id=args.installment_id,
                ),
                indent=2,
                ensure_ascii=True,
            )
        )
        return
    if args.cmd == "bookgen-generation-summary":
        print(
            json.dumps(
                bookgen.inspect_generation_summary(
                    project_id=args.project_id,
                    installment_id=args.installment_id,
                ),
                indent=2,
                ensure_ascii=True,
            )
        )
        return
    if args.cmd == "bookgen-approve":
        print(
            json.dumps(
                bookgen.approve_installment(
                    project_id=args.project_id,
                    installment_id=args.installment_id,
                    decision=args.decision,
                    note=args.note,
                ),
                indent=2,
                ensure_ascii=True,
            )
        )
        return
    if args.cmd == "bookgen-report":
        print(
            json.dumps(
                bookgen.operator_report(
                    project_id=args.project_id,
                    installment_id=args.installment_id,
                ),
                indent=2,
                ensure_ascii=True,
            )
        )
        return
    dispatch[args.cmd](args.run_date)


if __name__ == "__main__":
    main()
