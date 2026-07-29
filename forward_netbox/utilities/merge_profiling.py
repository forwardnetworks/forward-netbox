"""Low-overhead, opt-in instrumentation for repeatable merge profiling.

No recorder is active during normal syncs.  The profiling management command
activates one recorder around a real branch merge and writes anonymized JSONL.
"""

import contextvars
import os
import resource
import threading
import time
from collections import defaultdict
from contextlib import contextmanager

from django.db import connection

_ACTIVE_RECORDER = contextvars.ContextVar(
    "forward_merge_profile_recorder", default=None
)
_ACTIVE_SCOPE = contextvars.ContextVar("forward_merge_profile_scope", default=None)


def _rss_bytes():
    try:
        with open("/proc/self/statm", encoding="ascii") as handle:
            resident_pages = int(handle.read().split()[1])
        return resident_pages * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError, IndexError):
        return 0


class MergeProfileRecorder:
    """Measure wall/CPU/RSS and SQL round trips by active merge scope."""

    def __init__(self, *, metadata=None, sample_interval_seconds=0.02):
        self.metadata = dict(metadata or {})
        self.sample_interval_seconds = max(0.005, float(sample_interval_seconds))
        self._buckets = defaultdict(
            lambda: {
                "scope_entries": 0,
                "rows": 0,
                "wall_seconds": 0.0,
                "statements": 0,
                "db_wall_seconds": 0.0,
                "executemany_calls": 0,
                "sql_verbs": defaultdict(int),
            }
        )
        self._start_wall = None
        self._start_epoch = None
        self._start_cpu = None
        self._end_wall = None
        self._end_epoch = None
        self._end_cpu = None
        self._peak_rss_bytes = 0
        self._rss_stop = threading.Event()
        self._rss_thread = None

    @staticmethod
    def _key(scope_node):
        scope = (scope_node or {}).get("scope", scope_node or {})
        return (
            str(scope.get("phase") or "unattributed"),
            str(scope.get("model") or ""),
            str(scope.get("owner") or "unknown"),
        )

    @contextmanager
    def scope(self, phase, *, model="", owner="ours", rows=0):
        scope = {
            "phase": str(phase),
            "model": str(model or ""),
            "owner": str(owner or "unknown"),
        }
        key = self._key(scope)
        bucket = self._buckets[key]
        bucket["scope_entries"] += 1
        bucket["rows"] += max(0, int(rows or 0))
        parent = _ACTIVE_SCOPE.get()
        scope_node = {"scope": scope, "child_wall_seconds": 0.0}
        token = _ACTIVE_SCOPE.set(scope_node)
        started = time.perf_counter()
        try:
            yield
        finally:
            duration = time.perf_counter() - started
            bucket["wall_seconds"] += max(
                0.0,
                duration - scope_node["child_wall_seconds"],
            )
            _ACTIVE_SCOPE.reset(token)
            if parent is not None:
                parent["child_wall_seconds"] += duration

    def execute(self, execute, sql, params, many, context):
        key = self._key(_ACTIVE_SCOPE.get())
        bucket = self._buckets[key]
        started = time.perf_counter()
        try:
            return execute(sql, params, many, context)
        finally:
            bucket["statements"] += 1
            bucket["executemany_calls"] += int(bool(many))
            bucket["db_wall_seconds"] += time.perf_counter() - started
            verb = str(sql or "").lstrip().split(None, 1)
            bucket["sql_verbs"][verb[0].upper() if verb else "UNKNOWN"] += 1

    def _sample_rss(self):
        while not self._rss_stop.wait(self.sample_interval_seconds):
            self._peak_rss_bytes = max(self._peak_rss_bytes, _rss_bytes())

    @contextmanager
    def activate(self):
        if _ACTIVE_RECORDER.get() is not None:
            raise RuntimeError("A merge profile recorder is already active.")
        self._start_epoch = time.time()
        self._start_wall = time.perf_counter()
        self._start_cpu = time.process_time()
        self._peak_rss_bytes = _rss_bytes()
        self._rss_stop.clear()
        self._rss_thread = threading.Thread(
            target=self._sample_rss,
            name="forward-merge-rss-sampler",
            daemon=True,
        )
        self._rss_thread.start()
        recorder_token = _ACTIVE_RECORDER.set(self)
        try:
            with connection.execute_wrapper(self.execute):
                yield self
        finally:
            _ACTIVE_RECORDER.reset(recorder_token)
            self._end_cpu = time.process_time()
            self._end_wall = time.perf_counter()
            self._end_epoch = time.time()
            self._rss_stop.set()
            self._rss_thread.join(timeout=1.0)
            self._peak_rss_bytes = max(self._peak_rss_bytes, _rss_bytes())

    def result(self):
        wall = max(0.0, (self._end_wall or 0.0) - (self._start_wall or 0.0))
        scoped_wall = sum(values["wall_seconds"] for values in self._buckets.values())
        unattributed = self._buckets[("unattributed", "", "unknown")]
        unattributed["wall_seconds"] += max(0.0, wall - scoped_wall)
        buckets = []
        for (phase, model, owner), values in sorted(self._buckets.items()):
            item = {
                "phase": phase,
                "model": model,
                "owner": owner,
                **{key: value for key, value in values.items() if key != "sql_verbs"},
                "sql_verbs": dict(sorted(values["sql_verbs"].items())),
            }
            buckets.append(item)
        statement_count = sum(item["statements"] for item in buckets)
        db_wall = sum(item["db_wall_seconds"] for item in buckets)
        cpu = max(0.0, (self._end_cpu or 0.0) - (self._start_cpu or 0.0))
        return {
            **self.metadata,
            "started_epoch": self._start_epoch,
            "finished_epoch": self._end_epoch,
            "wall_seconds": wall,
            "python_cpu_seconds": cpu,
            "python_cpu_utilization": cpu / wall if wall else 0.0,
            "db_execute_wall_seconds": db_wall,
            "db_execute_wall_fraction": db_wall / wall if wall else 0.0,
            "statements": statement_count,
            "peak_rss_bytes": self._peak_rss_bytes,
            "process_peak_rss_bytes": int(
                resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            )
            * 1024,
            "buckets": buckets,
        }


@contextmanager
def profile_scope(phase, *, model="", owner="ours", rows=0):
    recorder = _ACTIVE_RECORDER.get()
    if recorder is None:
        yield
        return
    with recorder.scope(phase, model=model, owner=owner, rows=rows):
        yield
