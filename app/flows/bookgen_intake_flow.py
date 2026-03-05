from prefect import flow, task

from app.services import bookgen


@task(name="bookgen_intake_task")
def bookgen_intake_task(
    project_id: str,
    run_date: str,
    bookspec_key: str | None = None,
    bookspec_path: str | None = None,
) -> dict:
    return bookgen.run_intake(
        project_id=project_id,
        run_date=run_date,
        bookspec_key=bookspec_key,
        bookspec_path=bookspec_path,
    )


@flow(name="bookgen-intake")
def bookgen_intake_flow(
    project_id: str,
    run_date: str,
    bookspec_key: str | None = None,
    bookspec_path: str | None = None,
) -> dict:
    return bookgen_intake_task(project_id, run_date, bookspec_key, bookspec_path)

