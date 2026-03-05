from prefect import flow, task

from app.services import render_markdown


@task(retries=1, retry_delay_seconds=15, log_prints=True)
def render_task(run_date: str) -> dict:
    return render_markdown.run(run_date)


@flow(name="slw-dailycast-render")
def render_flow(run_date: str) -> dict:
    return render_task(run_date)
