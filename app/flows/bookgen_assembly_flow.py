from prefect import flow, task

from app.services import bookgen


@task(name="bookgen_assembly_export_task")
def bookgen_assembly_export_task(project_id: str) -> dict:
    return bookgen.run_assembly_export(project_id=project_id)


@flow(name="bookgen-assembly")
def bookgen_assembly_flow(project_id: str) -> dict:
    return bookgen_assembly_export_task(project_id)

