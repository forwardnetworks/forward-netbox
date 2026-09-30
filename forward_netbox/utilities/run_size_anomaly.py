"""Is this run far larger than what this model normally sees?

A sync that stages ten times a model's usual change volume, or several times
the rows NetBox already holds for it, is either the first honest load of
something that was missing or a matching fault that is about to write
duplicates. The two look identical until somebody counts, and with Auto merge
on nobody counted before the merge started. This is the arithmetic that lets
the Health page, the sync page and the merge step say so in plain words.

Pure functions over plain numbers, so the rule can be tested exhaustively and
read in one place. Nothing here imports Django or NetBox.

Two norms, either of which makes a model's normal size *established*:

- **The table itself.** A model whose NetBox table already holds at least
  ``MIN_ESTABLISHED_ROWS`` rows has a size this estate has settled on. A run
  that stages ``GROWTH_MULTIPLE`` times that in changes is not steady state. An
  empty or small table has no established size, so a first load into it is
  never flagged - that is what a first baseline is.
- **Prior runs.** When at least ``HISTORY_MIN_RUNS`` earlier runs recorded how
  many changes they applied to the model, the median is its typical run. A run
  ``HISTORY_MULTIPLE`` times that is flagged.

Both also require ``MIN_CHANGES`` absolute changes, so a model that normally
moves twenty rows and moved two hundred is left alone.

Why not the learned change density: it is changes per *written* row, so it says
how many changes one written row produces, never how many rows a run writes.
The run this exists for wrote 127k BGP peer rows at a density that looked
entirely normal.
"""

from statistics import median

# A run must stage at least this many changes for a model to be flagged, so a
# small model's ordinary churn never trips a multiple.
MIN_CHANGES = 10_000
# NetBox must already hold this many rows of the model for its table size to
# count as established.
MIN_ESTABLISHED_ROWS = 1_000
# Staged changes as a multiple of the rows NetBox already holds.
GROWTH_MULTIPLE = 3.0
# Staged changes as a multiple of the typical earlier run.
HISTORY_MULTIPLE = 10.0
# Earlier runs needed before their median counts as a norm.
HISTORY_MIN_RUNS = 3

# Where a held run's findings are recorded on the ingestion's job, so Health,
# the sync page and the support bundle can say why a merge was not automatic.
HOLD_JOB_DATA_KEY = "run_size_hold"

BASIS_TABLE = "netbox_rows"
BASIS_HISTORY = "prior_runs"


def assess_run_size(models, *, history=None):
    """Return one finding per model whose staged changes are out of proportion.

    ``models`` maps a model string to ``{"changes": int, "existing_rows": int
    or None}``. ``history`` maps a model string to the change counts earlier
    runs applied to it. Findings are ordered largest first, each a plain dict
    that is safe to persist and to put in a support bundle: counts only.
    """
    history = history or {}
    findings = []
    for model_string, facts in (models or {}).items():
        changes = _count(facts.get("changes"))
        if changes < MIN_CHANGES:
            continue
        candidates = []
        existing = facts.get("existing_rows")
        if isinstance(existing, int) and existing >= MIN_ESTABLISHED_ROWS:
            multiple = changes / existing
            if multiple >= GROWTH_MULTIPLE:
                candidates.append(
                    _finding(
                        model_string,
                        changes,
                        multiple,
                        BASIS_TABLE,
                        norm=existing,
                        runs=None,
                    )
                )
        runs = [_count(value) for value in history.get(model_string, ())]
        runs = [value for value in runs if value > 0]
        if len(runs) >= HISTORY_MIN_RUNS:
            typical = max(1, int(median(runs)))
            multiple = changes / typical
            if multiple >= HISTORY_MULTIPLE:
                candidates.append(
                    _finding(
                        model_string,
                        changes,
                        multiple,
                        BASIS_HISTORY,
                        norm=typical,
                        runs=len(runs),
                    )
                )
        if candidates:
            findings.append(max(candidates, key=lambda item: item["multiple"]))
    return sorted(findings, key=lambda item: (-item["changes"], item["model"]))


def describe_finding(finding):
    """One plain sentence for a finding, naming the model and the multiple."""
    model = finding["model"]
    changes = f"{finding['changes']:,}"
    multiple = f"{finding['multiple']:.1f}"
    norm = f"{finding['norm']:,}"
    if finding["basis"] == BASIS_TABLE:
        return (
            f"{model} would stage {changes} changes, {multiple}x the {norm} "
            "rows NetBox already holds for it."
        )
    return (
        f"{model} staged {changes} changes, {multiple}x its typical run of "
        f"{norm} (the median of the last {finding['runs']})."
    )


def _finding(model, changes, multiple, basis, *, norm, runs):
    return {
        "model": model,
        "changes": changes,
        "multiple": round(multiple, 1),
        "basis": basis,
        "norm": norm,
        "runs": runs,
    }


def _count(value):
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0
