from functools import lru_cache

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    minio_endpoint: str = Field(alias="MINIO_ENDPOINT")
    minio_access_key: str = Field(alias="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field(alias="MINIO_SECRET_KEY")
    minio_secure: bool = Field(default=False, alias="MINIO_SECURE")
    minio_local_endpoint: str | None = Field(default="http://127.0.0.1:19000", alias="MINIO_LOCAL_ENDPOINT")

    pg_host: str = Field(alias="PG_HOST")
    pg_port: int = Field(default=5432, alias="PG_PORT")
    pg_user: str = Field(alias="PG_USER")
    pg_password: str = Field(alias="PG_PASSWORD")
    pg_db: str = Field(alias="PG_DB")

    mlflow_tracking_uri: str = Field(alias="MLFLOW_TRACKING_URI")
    mlflow_local_tracking_uri: str | None = Field(default="http://127.0.0.1:15000", alias="MLFLOW_LOCAL_TRACKING_URI")
    prefect_api_url: str = Field(alias="PREFECT_API_URL")
    prefect_local_api_url: str | None = Field(default="http://127.0.0.1:14200/api", alias="PREFECT_LOCAL_API_URL")

    llm_endpoint: str | None = Field(default=None, alias="LLM_ENDPOINT")
    llm_api_key: str | None = Field(default=None, alias="LLM_API_KEY")
    llm_model: str | None = Field(default=None, alias="LLM_MODEL")
    llm_provider_profile: str = Field(default="default", alias="LLM_PROVIDER_PROFILE")
    llm_reasoning_effort: str | None = Field(default=None, alias="LLM_REASONING_EFFORT")
    llm_timeout_seconds: int = Field(default=60, alias="LLM_TIMEOUT_SECONDS")
    llm_max_retries: int = Field(default=4, alias="LLM_MAX_RETRIES")
    llm_strict_mode: bool = Field(default=True, alias="LLM_STRICT_MODE")
    bookgen_generation_preset: str = Field(default="production", alias="BOOKGEN_GENERATION_PRESET")
    bookgen_use_llm: bool = Field(default=True, alias="BOOKGEN_USE_LLM")
    bookgen_llm_chapter_limit: int = Field(default=0, alias="BOOKGEN_LLM_CHAPTER_LIMIT")
    bookgen_eval_use_llm: bool = Field(default=True, alias="BOOKGEN_EVAL_USE_LLM")
    bookgen_eval_llm_chapter_limit: int = Field(default=0, alias="BOOKGEN_EVAL_LLM_CHAPTER_LIMIT")
    bookgen_rewrite_use_llm: bool = Field(default=True, alias="BOOKGEN_REWRITE_USE_LLM")
    bookgen_rewrite_llm_chapter_limit: int = Field(default=0, alias="BOOKGEN_REWRITE_LLM_CHAPTER_LIMIT")
    bookgen_title_critic_use_llm: bool = Field(default=False, alias="BOOKGEN_TITLE_CRITIC_USE_LLM")
    bookgen_title_critic_shortlist_size: int = Field(default=5, alias="BOOKGEN_TITLE_CRITIC_SHORTLIST_SIZE")
    bookgen_structural_retry_limit: int = Field(default=0, alias="BOOKGEN_STRUCTURAL_RETRY_LIMIT")
    bookgen_editorial_stage_gate: bool = Field(default=True, alias="BOOKGEN_EDITORIAL_STAGE_GATE")
    bookgen_allow_lock_override: bool = Field(default=False, alias="BOOKGEN_ALLOW_LOCK_OVERRIDE")
    factpack_use_llm: bool = Field(default=False, alias="FACTPACK_USE_LLM")
    allow_synthetic_fallback: bool = Field(default=False, alias="ALLOW_SYNTHETIC_FALLBACK")
    transcript_require_llm: bool = Field(default=True, alias="TRANSCRIPT_REQUIRE_LLM")
    transcript_min_words: int = Field(default=4500, alias="TRANSCRIPT_MIN_WORDS")
    transcript_max_words: int = Field(default=6500, alias="TRANSCRIPT_MAX_WORDS")
    transcript_wpm_baseline: int = Field(default=145, alias="TRANSCRIPT_WPM_BASELINE")
    transcript_runtime_target_min: float = Field(default=30.0, alias="TRANSCRIPT_RUNTIME_TARGET_MIN")
    transcript_runtime_target_max: float = Field(default=45.0, alias="TRANSCRIPT_RUNTIME_TARGET_MAX")
    transcript_runtime_soft_min: float = Field(default=28.0, alias="TRANSCRIPT_RUNTIME_SOFT_MIN")
    transcript_runtime_soft_max: float = Field(default=47.0, alias="TRANSCRIPT_RUNTIME_SOFT_MAX")
    transcript_min_host_turns: int = Field(default=140, alias="TRANSCRIPT_MIN_HOST_TURNS")
    transcript_max_robotic_phrase_hits: int = Field(default=8, alias="TRANSCRIPT_MAX_ROBOTIC_PHRASE_HITS")
    transcript_max_final_score_hits: int = Field(default=8, alias="TRANSCRIPT_MAX_FINAL_SCORE_HITS")
    transcript_max_repetitive_starter_hits: int = Field(
        default=18,
        alias="TRANSCRIPT_MAX_REPETITIVE_STARTER_HITS",
    )

    # Generic/custom adapters
    sports_api_url: str | None = Field(default=None, alias="SPORTS_API_URL")
    sports_api_key: str | None = Field(default=None, alias="SPORTS_API_KEY")
    news_api_url: str | None = Field(default=None, alias="NEWS_API_URL")
    news_api_key: str | None = Field(default=None, alias="NEWS_API_KEY")

    # Provider-specific sources
    sportsdb_api_url: str | None = Field(
        default=None,
        validation_alias=AliasChoices("SPORTSDB_API_URL", "SPORTS_API_URL"),
    )
    sportsdb_api_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices("SPORTSDB_API_KEY", "SPORTS_API_KEY"),
    )
    espn_site_api_url: str = Field(default="https://site.api.espn.com", alias="ESPN_SITE_API_URL")
    espn_core_api_url: str | None = Field(default=None, alias="ESPN_CORE_API_URL")
    espn_sports: str = Field(
        default="football/nfl,football/college-football,basketball/nba,basketball/wnba,baseball/mlb,hockey/nhl,basketball/mens-college-basketball,basketball/womens-college-basketball",
        alias="ESPN_SPORTS",
    )
    include_preseason_scores: bool = Field(default=False, alias="INCLUDE_PRESEASON_SCORES")
    include_spring_training_scores: bool = Field(default=False, alias="INCLUDE_SPRING_TRAINING_SCORES")
    newsapi_url: str = Field(
        default="https://newsapi.org/v2",
        validation_alias=AliasChoices("NEWSAPI_URL", "NEWS_API_URL"),
    )
    newsapi_api_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices("NEWSAPI_API_KEY", "NEWS_API_KEY"),
    )
    gnews_url: str = Field(
        default="https://gnews.io/api/v4",
        validation_alias=AliasChoices("GNEWS_URL", "GNEWS_API_URL"),
    )
    gnews_api_key: str | None = Field(default=None, alias="GNEWS_API_KEY")

    s3_bucket: str = Field(default="slw-dailycast", alias="S3_BUCKET")
    lakefs_enabled: bool = Field(default=False, alias="LAKEFS_ENABLED")
    lakefs_endpoint: str | None = Field(default=None, alias="LAKEFS_ENDPOINT")
    lakefs_local_endpoint: str | None = Field(default="http://127.0.0.1:18000", alias="LAKEFS_LOCAL_ENDPOINT")
    lakefs_repo: str | None = Field(default=None, alias="LAKEFS_REPO")
    lakefs_access_key: str | None = Field(default=None, alias="LAKEFS_ACCESS_KEY")
    lakefs_secret_key: str | None = Field(default=None, alias="LAKEFS_SECRET_KEY")
    lakefs_source_branch: str = Field(default="main", alias="LAKEFS_SOURCE_BRANCH")
    lakefs_bookgen_branch_prefix: str = Field(default="bookgen", alias="LAKEFS_BOOKGEN_BRANCH_PREFIX")
    lakefs_dailycast_branch_prefix: str = Field(default="run", alias="LAKEFS_DAILYCAST_BRANCH_PREFIX")

    pgvector_conninfo: str | None = Field(default=None, alias="PGVECTOR_CONNINFO")
    embedding_model: str | None = Field(default=None, alias="EMBEDDING_MODEL")
    rag_top_k: int = Field(default=10, alias="RAG_TOP_K")
    rag_min_score: float = Field(default=0.15, alias="RAG_MIN_SCORE")

    elevenlabs_api_key: str | None = Field(default=None, alias="ELEVENLABS_API_KEY")
    elevenlabs_enabled: bool = Field(default=False, alias="ELEVENLABS_ENABLED")
    eleven_voice_evan: str | None = Field(default=None, alias="ELEVEN_VOICE_EVAN")
    eleven_voice_marcus: str | None = Field(default=None, alias="ELEVEN_VOICE_MARCUS")
    eleven_voice_tyler: str | None = Field(default=None, alias="ELEVEN_VOICE_TYLER")
    eleven_voice_darius: str | None = Field(default=None, alias="ELEVEN_VOICE_DARIUS")
    eleven_voice_caleb: str | None = Field(default=None, alias="ELEVEN_VOICE_CALEB")
    eleven_voice_lucas: str | None = Field(default=None, alias="ELEVEN_VOICE_LUCAS")

    article_output_dir: str = Field(default="/var/lib/slw/artifacts/articles", alias="ARTICLE_OUTPUT_DIR")

    log_level: str = Field(default="INFO", alias="LOG_LEVEL")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
