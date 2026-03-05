from prefect import flow, task

from app.services import quality_gate, verify_factpack


@task(retries=1, retry_delay_seconds=15, log_prints=True)
def verify_factpack_task(run_date: str) -> dict:
    return verify_factpack.run(run_date)


@task(retries=0, log_prints=True)
def quality_gate_task(run_date: str) -> dict:
    return quality_gate.run(run_date)


@flow(name="slw-dailycast-verify")
def verify_flow(run_date: str) -> dict:
    verify_res = verify_factpack_task(run_date)
    quality_res = quality_gate_task(run_date)
    return {"verify": verify_res, "quality_gate": quality_res}
