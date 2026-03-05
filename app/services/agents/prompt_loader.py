from pathlib import Path


_PROMPT_ROOT = Path(__file__).resolve().parent / "prompts"


def load_prompt(filename: str) -> str:
    path = (_PROMPT_ROOT / filename).resolve()
    if not str(path).startswith(str(_PROMPT_ROOT.resolve())):
        raise ValueError(f"prompt path escapes prompt root: {filename}")
    return path.read_text(encoding="utf-8")
