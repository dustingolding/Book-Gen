from datetime import datetime

from pydantic import BaseModel, Field


class IngestRecord(BaseModel):
    run_date: str
    source: str
    record_id: str
    title: str
    content: dict
    captured_at: datetime = Field(default_factory=datetime.utcnow)


class NormalizedEvent(BaseModel):
    run_date: str
    event_id: str
    league: str
    event_type: str
    title: str
    summary: str
    entities: list[str]
    metrics: dict
    citations: list[str]


class RankedEvent(BaseModel):
    event_id: str
    score: float
    rationale: str


class Briefpack(BaseModel):
    run_date: str
    top_scores: list[dict]
    headlines: list[dict]
    upcoming_matchups: list[dict]
    citations: list[str]


class TranscriptSegment(BaseModel):
    segment: str
    text: str
    citations: list[str]


class Transcript(BaseModel):
    run_date: str
    title: str
    language: str = "en-US"
    segments: list[TranscriptSegment]
    citation_coverage: float
    numeric_fidelity: float
    verified: bool
    generated_at: datetime = Field(default_factory=datetime.utcnow)
