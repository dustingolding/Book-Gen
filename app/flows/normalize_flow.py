from prefect import flow, task

from app.services import normalize


@task(retries=2, retry_delay_seconds=20, log_prints=True)
def normalize_task(run_date: str) -> dict:
    return normalize.run(run_date)


@flow(name="slw-dailycast-normalize")
def normalize_flow(run_date: str) -> dict:
    return normalize_task(run_date)
