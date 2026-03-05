from prefect import flow, task

from app.services import agent_transcript


@task(retries=1, retry_delay_seconds=15, log_prints=True)
def transcript_task(run_date: str) -> dict:
    return agent_transcript.run(run_date)


@flow(name="slw-dailycast-agent")
def agent_flow(run_date: str) -> dict:
    return transcript_task(run_date)
