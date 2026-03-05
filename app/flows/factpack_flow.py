from prefect import flow, task

from app.services import factpack


@task(retries=2, retry_delay_seconds=20, log_prints=True)
def factpack_task(run_date: str) -> dict:
    return factpack.run(run_date)


@flow(name="slw-dailycast-factpack")
def factpack_flow(run_date: str) -> dict:
    return factpack_task(run_date)
