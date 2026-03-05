from prefect import flow, task

from app.services import factpack


@task(retries=2, retry_delay_seconds=20, log_prints=True)
def briefpack_task(run_date: str) -> dict:
    return factpack.run(run_date)


@flow(name="slw-dailycast-briefpack")
def briefpack_flow(run_date: str) -> dict:
    return briefpack_task(run_date)
