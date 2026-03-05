from prefect import flow, task

from app.services import bookgen


@task(name="bookgen_draft_chapters_task")
def bookgen_draft_chapters_task(project_id: str) -> dict:
    return bookgen.run_chapter_drafting(project_id=project_id)


@task(name="bookgen_review_chapters_task")
def bookgen_review_chapters_task(project_id: str) -> dict:
    return bookgen.run_chapter_review(project_id=project_id)


@flow(name="bookgen-chapters")
def bookgen_chapters_flow(project_id: str) -> dict:
    draft = bookgen_draft_chapters_task(project_id)
    review = bookgen_review_chapters_task(project_id)
    return {"draft": draft, "review": review}

