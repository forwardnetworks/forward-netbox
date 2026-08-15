"""Count how many objects actually differ, by running the apply classification.

The drift report used to call every fetched row a change, because the dependency
preview had nothing to compare against and said so with
``change_estimate_kind = "workload_upper_bound"``. That made ``In sync``,
``Drifted models`` and ``Total drift`` read "Not measured" on every run for
every deployment, permanently - a promise the UI could not keep.

The comparison here is not a reimplementation. It calls
``bulk_orm_apply_simple_models(..., preview=True)``, which normalises, resolves
and compares exactly as the apply does and returns before it writes. A separate
normaliser would drift from the real one, and the symptom would be a drift
figure wrong in whichever direction is least noticeable - worse than no figure,
because an operator would act on it.
"""


class _PreviewStatistics:
    """Absorb the per-row outcome calls the classification makes."""

    def __init__(self):
        self.outcomes = {}

    def increment_statistics(self, model_string, *, outcome="applied", amount=1):
        amount = max(0, int(amount or 0))
        if amount <= 0:
            return
        self.outcomes[outcome] = self.outcomes.get(outcome, 0) + amount

    def log_warning(self, *args, **kwargs):
        return None


class _PreviewEventsClearer:
    """The classification increments this per row; a preview clears no events."""

    def __init__(self):
        self.count = 0

    def increment(self):
        self.count += 1

    def clear(self):
        return None


class PreviewRunner:
    """A runner that records nothing and writes nothing.

    The classification reports unusable rows through ``_record_issue`` and moves
    per-model counters through ``logger``. Handing it the real runner would file
    ingestion issues and move statistics for a sync that never ran, so it gets
    this instead and the rejected rows are counted rather than persisted.
    """

    def __init__(self, sync=None):
        self.sync = sync
        self.logger = _PreviewStatistics()
        self.events_clearer = _PreviewEventsClearer()
        self.rejected_rows = 0

    def _record_issue(self, model_string, message, row, context=None):
        self.rejected_rows += 1


def compare_model_rows(sync, model_string, rows):
    """Return ``{"creates", "updates", "unchanged", "rejected"}`` or ``None``.

    ``None`` means this model has no comparison yet - the bespoke bulk paths and
    every adapter-only model - and the caller must keep reporting an upper bound
    for it. Reporting a confident zero for something never compared is the one
    failure mode here with a real consequence, because it would tell an operator
    they are in sync when nothing checked.
    """
    from .apply_engine_bulk import bulk_orm_apply_simple_models

    if not rows:
        return {"creates": 0, "updates": 0, "unchanged": 0, "rejected": 0}
    runner = PreviewRunner(sync=sync)
    counts = bulk_orm_apply_simple_models(
        runner,
        model_string,
        list(rows),
        preview=True,
    )
    if not isinstance(counts, dict):
        # `None` is the documented "no comparison" answer, but the apply path
        # also has plain-bool exits for its own callers. Anything that is not a
        # count mapping is treated as "not compared" rather than coerced, so a
        # path this has not audited can never surface as zero drift.
        return None
    return {**counts, "rejected": runner.rejected_rows}
