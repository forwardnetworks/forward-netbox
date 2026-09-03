# Binding a criterion to a committed query version, and evaluating it.
#
# Binding is the gate that makes a verdict mean anything. An unbound criterion
# is evaluated against whatever its query says at the time, so the assertion
# could move under the change it is judging - and the before/after comparison
# would silently compare two different questions.
from dataclasses import dataclass

from ..utilities.query_execution_contract import query_source_sha256
from .choices import ForwardCriterionExpectationChoices as Expectation


@dataclass(frozen=True)
class BindResult:
    bound: bool
    query_id: str = ""
    commit_id: str = ""
    source_sha256: str = ""
    error: str = ""


def bind_criterion(criterion, client) -> BindResult:
    """Resolve a criterion's query path to a concrete id at a concrete commit.

    Reuses `resolve_nqe_query_reference`, which is the same resolution the sync
    already performs for NQE maps. A criterion is not a map - it binds a query
    to an ASSERTION rather than to a NetBox model, and is constrained to no
    model list - but the resolution is identical and duplicating it would mean
    two places to get commit pinning wrong.
    """
    if not criterion.query_path:
        return BindResult(False, error="The criterion has no query path to bind.")
    try:
        resolved = client.resolve_nqe_query_reference(
            repository="org",
            query_path=criterion.query_path,
            commit_id=criterion.commit_id or None,
        )
    except Exception as exc:  # noqa: BLE001 - surfaced to the operator verbatim
        return BindResult(False, error=f"{type(exc).__name__}: {exc}")

    query_id = str(resolved.get("queryId") or resolved.get("query_id") or "").strip()
    commit_id = str(resolved.get("commitId") or resolved.get("commit_id") or "").strip()
    if not query_id or not commit_id:
        return BindResult(
            False,
            error=(
                "Forward resolved the path but returned no query id and commit "
                "pair, so the criterion cannot be pinned to a version."
            ),
        )
    return BindResult(
        bound=True,
        query_id=query_id,
        commit_id=commit_id,
        source_sha256=query_source_sha256(resolved.get("source")),
    )


def evaluate_expectation(expectation: str, rows, baseline_rows=None) -> bool:
    """Turn rows into pass or fail.

    Deliberately three narrow rules rather than an expression language. A
    criterion an operator cannot predict the meaning of is a criterion they
    will stop trusting, and every rule here fits in a sentence on the page.
    """
    count = len(rows or ())
    if expectation == Expectation.NO_ROWS:
        return count == 0
    if expectation == Expectation.SOME_ROWS:
        return count > 0
    if expectation == Expectation.NO_DIFF:
        # Compared against the baseline's rows rather than against zero: a
        # query that legitimately returns rows in both phases preserves state
        # as long as the SET has not moved.
        return _row_key_set(rows) == _row_key_set(baseline_rows)
    raise ValueError(f"unknown expectation {expectation!r}")


def _row_key_set(rows) -> frozenset:
    """A comparable, order-independent view of a result set.

    Values are hashed into the key rather than kept, because evidence is
    support-bundle content and a diff must not become a place customer data
    accumulates.
    """
    keyed = set()
    for row in rows or ():
        if isinstance(row, dict):
            keyed.add(tuple(sorted((str(k), str(v)) for k, v in row.items())))
        else:
            keyed.add((("value", str(row)),))
    return frozenset(keyed)


def result_shape(rows) -> dict:
    """Schema identifiers only - never values.

    The same rule ingestion-issue diagnosis follows: key names are schema, and
    payload values are the customer's.
    """
    keys = set()
    for row in rows or ():
        if isinstance(row, dict):
            keys.update(str(k) for k in row)
    return {"columns": sorted(keys), "row_count": len(rows or ())}
