from datetime import date

from prefect import flow, get_run_logger

from app.flows.bookgen_assembly_flow import bookgen_assembly_flow
from app.flows.bookgen_chapters_flow import bookgen_chapters_flow
from app.flows.bookgen_intake_flow import bookgen_intake_flow
from app.flows.bookgen_planning_flow import bookgen_planning_flow


@flow(name="bookgen-parent")
def bookgen_parent_flow(
    project_id: str,
    run_date: str | None = None,
    bookspec_key: str | None = None,
    bookspec_path: str | None = None,
) -> dict:
    run_date = run_date or date.today().isoformat()
    logger = get_run_logger()

    intake = bookgen_intake_flow(
        project_id=project_id,
        run_date=run_date,
        bookspec_key=bookspec_key,
        bookspec_path=bookspec_path,
    )
    planning = bookgen_planning_flow(intake)
    chapters = bookgen_chapters_flow(project_id)
    assembly = bookgen_assembly_flow(project_id)

    logger.info(
        "bookgen_run_summary | project_id=%s run_date=%s chapter_count=%s drafted=%s skipped=%s pass_rate=%.2f",
        project_id,
        run_date,
        int(planning.get("outline", {}).get("chapter_count", 0)),
        int(chapters.get("draft", {}).get("drafted", 0)),
        int(chapters.get("draft", {}).get("skipped_existing", 0)),
        float(chapters.get("review", {}).get("chapter_pass_rate", 0.0)),
    )
    return {
        "project_id": project_id,
        "run_date": run_date,
        "status": "succeeded",
        "intake": intake,
        "planning": planning,
        "chapters": chapters,
        "assembly": assembly,
    }

