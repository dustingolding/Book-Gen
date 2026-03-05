from prefect import flow, task

from app.services import bookgen


@task(name="bookgen_resolve_prompt_pack_task")
def bookgen_resolve_prompt_pack_task(intake: dict) -> dict:
    return bookgen.run_prompt_pack_resolve(intake=intake)


@task(name="bookgen_bible_outline_task")
def bookgen_bible_outline_task(intake: dict, resolved: dict) -> dict:
    return bookgen.run_bible_outline(intake=intake, resolved=resolved)


@flow(name="bookgen-planning")
def bookgen_planning_flow(intake: dict) -> dict:
    resolved = bookgen_resolve_prompt_pack_task(intake)
    outline = bookgen_bible_outline_task(intake, resolved)
    return {"resolved": resolved, "outline": outline}

