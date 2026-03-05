from prefect import flow, task

from app.services.agents.realism_naturalizer import run


@task(retries=1, retry_delay_seconds=15, log_prints=True)
def realism_task(run_date: str) -> dict:
    return run(run_date)


@flow(name="slw-dailycast-realism")
def realism_flow(run_date: str) -> dict:
    return realism_task(run_date)
