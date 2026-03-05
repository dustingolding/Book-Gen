from prefect import flow, task

from app.services import rank


@task(retries=2, retry_delay_seconds=20, log_prints=True)
def rank_task(run_date: str) -> dict:
    return rank.run(run_date)


@flow(name="slw-dailycast-rank")
def rank_flow(run_date: str) -> dict:
    return rank_task(run_date)
