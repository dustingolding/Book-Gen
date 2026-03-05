from prefect import flow, task

from app.services.agents.length_adjuster import adjust_length_if_needed


@task(retries=1, retry_delay_seconds=15, log_prints=True)
def length_adjust_task(run_date: str) -> dict:
    return adjust_length_if_needed(run_date)


@flow(name="slw-dailycast-length-adjust")
def length_adjust_flow(run_date: str) -> dict:
    return length_adjust_task(run_date)
