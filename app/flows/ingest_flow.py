from prefect import flow, task

from app.services import ingest_news, ingest_sports


@task(retries=3, retry_delay_seconds=30, log_prints=True)
def ingest_sports_task(run_date: str) -> dict:
    return ingest_sports.run(run_date)


@task(retries=3, retry_delay_seconds=30, log_prints=True)
def ingest_news_task(run_date: str) -> dict:
    return ingest_news.run(run_date)


@flow(name="slw-dailycast-ingest")
def ingest_flow(run_date: str) -> dict:
    sports_future = ingest_sports_task.submit(run_date)
    news_future = ingest_news_task.submit(run_date)
    return {
        "sports": sports_future.result(),
        "news": news_future.result(),
    }
