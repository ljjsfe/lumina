"""Fixed dev sets for fast iteration.

Design principle: stratified by difficulty, then shifted harder.
- Gives a conservatively pessimistic signal (don't be fooled by easy wins)
- Still has easy tasks so you can detect regressions there too
- Fixed IDs — reproducible across runs for fair comparison

KDD dev set (10 tasks):
  Distribution: 2 easy + 2 medium + 4 hard + 2 extreme  (vs. full: 30/46/20/4%)
  Domains covered: medical, club/events, F1, toxicology/chemistry, superheroes, cards

DABstep dev set (10 tasks):
  All 10 tasks from the official dev split (has gold answers)
  Distribution: 3 easy + 7 hard
  Domain: Adyen financial payments data
"""

from __future__ import annotations


# --- KDD dev set ---
# 2 easy: different domains (medical + F1)
# 3 medium: different domains (events, chemistry, translation)
# 3 hard: different domains (match, toxicology, superheroes)
# 2 extreme: both available extreme tasks
KDD_DEV: tuple[str, ...] = (
    # easy (2)
    "task_11",    # medical: thrombosis severity patients
    "task_86",    # F1: Alex Yoong track number
    # medium (3)
    "task_145",   # events: Student_Club attendance
    "task_292",   # F1: constructor highest points in race
    "task_379",   # chemistry: toxicology atoms (borderline hard)
    # hard (3)
    "task_330",   # sports: match score multi-join
    "task_396",   # superheroes: height range + percentage
    "task_408",   # F1: lap speed percentage comparison
    # extreme (2)
    "task_418",   # medical: creatinine abnormal patients
    "task_420",   # cards: commander/legal format percentage
)

# --- DABstep dev set ---
# All 10 tasks from the official HuggingFace dev split (adyen/DABstep)
# These are the only tasks with gold answers available locally
DABSTEP_DEV: tuple[str, ...] = ("5", "49", "70", "1273", "1305", "1464", "1681", "1753", "1871", "2697")


def get_dev_set(benchmark: str) -> tuple[str, ...]:
    """Return the fixed dev set for a benchmark."""
    if benchmark == "dabstep":
        return DABSTEP_DEV
    return KDD_DEV


def describe_dev_set(benchmark: str) -> str:
    """Human-readable summary of dev set composition."""
    if benchmark == "dabstep":
        return (
            "DABstep dev (10 tasks): official HF dev split  "
            "→ 3 easy + 7 hard (Adyen payments domain)"
        )
    return (
        "KDD dev (10 tasks): "
        "2 easy | 2 medium | 4 hard | 2 extreme  "
        "→ harder-biased vs. full distribution (30/46/20/4%)"
    )
