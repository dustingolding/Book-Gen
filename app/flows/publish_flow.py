from prefect import flow, task

from app.services import publish


@task(retries=1, retry_delay_seconds=15, log_prints=True)
def publish_task(run_date: str) -> dict:
    return publish.run(run_date)


@flow(name="slw-dailycast-publish")
def publish_flow(run_date: str) -> dict:
    return publish_task(run_date)
