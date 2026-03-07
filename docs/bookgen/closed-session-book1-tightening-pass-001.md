# Closed Session Book 1 Tightening Pass 001

Project: `closed-session-book1-prod-001`  
Installment: `book-01`  
Generated UTC: `2026-03-07T03:26Z`

## Goal

Run a controlled editorial polish pass focused on:

- voice stability variance
- repeated sentence/phrase texture
- minor theme reinforcement where flagged

Baseline manuscript is already locked, so execute polish on a new project id cloned from the same bookspec.

## Priority Queue

1. `ch-18` (highest risk)
- voice_stability: `6.55`
- repeated_sentence_count: `9`
- signals: `character_voice_shift`, `voice_repetition`
- note: underlength relative to chapter target (`2354` words)

2. `ch-04`
- voice_stability: `7.16`
- repeated_sentence_count: `36`
- signals: `voice_repetition`, `theme_absence`

3. `ch-08`
- voice_stability: `7.23`
- repeated_sentence_count: `20`
- signal: `voice_repetition`

4. `ch-23`
- voice_stability: `7.32`
- repeated_sentence_count: `18`
- signal: `voice_repetition`

5. `ch-10`
- voice_stability: `7.17`
- repeated_sentence_count: `12`
- signal: `voice_repetition`

6. `ch-06`
- voice_stability: `7.20`
- repeated_sentence_count: `12`
- signal: `voice_repetition`

## Suggested Execution

Use a new project id, for example: `closed-session-book1-polish-001`.

Keep:
- same bookspec structure
- same production image pin
- LLM eval enabled
- deterministic rewrite (`--bookgen-rewrite-use-llm false`) for stability

Then compare:
- `chapter_pass_rate`
- `avg_overall_score`
- per-chapter `voice_stability`
- per-chapter `repeated_sentence_count`

Only promote polish run if it improves quality without continuity/publishability regressions.
