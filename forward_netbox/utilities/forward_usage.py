"""`ForwardClient`'s API-usage counters, extracted into their own module
(forward-sdk migration plan, step 2 of 7). No behavior change: this is a
direct extraction of `ForwardClient`'s usage-tracking methods and their
backing state into a self-contained class, still called by exactly the same
call sites through `ForwardClient`'s own (now delegating) methods of the
same name.

`evaluate_forward_api_usage` reads `observed_http_attempts_per_minute` from
`summary()`'s output and FAILs builds above the Forward SaaS hard block; this
class's summary shape must stay exactly what it is today.
"""

import hashlib
import json
import threading
import time


class ApiUsageTracker:
    def __init__(self, api_requests_per_minute):
        self.api_requests_per_minute = api_requests_per_minute
        self._lock = threading.Lock()
        self._first_http_attempt_at = None
        self._last_http_attempt_at = None
        self._usage = self._empty_usage()
        self._nqe_execution_signatures: dict[str, int] = {}

    def _empty_usage(self):
        return {
            "api_requests_per_minute": self.api_requests_per_minute,
            "http_attempts": 0,
            "http_successes": 0,
            "http_failures": 0,
            "http_timeout_failures": 0,
            "http_transport_failures": 0,
            "http_status_failures": 0,
            "http_transient_status_failures": 0,
            "http_nontransient_status_failures": 0,
            "http_429_failures": 0,
            "http_retries": 0,
            "http_status_classes": {},
            "throttle_sleep_seconds": 0.0,
            "nqe_query_calls": 0,
            "nqe_diff_calls": 0,
            "nqe_pages": 0,
            "nqe_query_pages": 0,
            "nqe_diff_pages": 0,
            "nqe_async_query_calls": 0,
            "nqe_async_trigger_calls": 0,
            "nqe_async_status_calls": 0,
            "nqe_async_result_calls": 0,
            "read_cache_hits": 0,
            "read_cache_misses": 0,
        }

    def record(self, key, amount=1):
        with self._lock:
            self._usage[key] = self._usage.get(key, 0) + amount

    def record_nqe_execution_signature(self, kind, identity):
        encoded = json.dumps(
            {"kind": kind, **identity},
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        signature = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        with self._lock:
            self._nqe_execution_signatures[signature] = (
                self._nqe_execution_signatures.get(signature, 0) + 1
            )

    def record_read_cache_hit(self):
        self.record("read_cache_hits")

    def record_read_cache_miss(self):
        self.record("read_cache_misses")

    def record_http_attempt(self):
        now = time.monotonic()
        with self._lock:
            self._usage["http_attempts"] = self._usage.get("http_attempts", 0) + 1
            if self._first_http_attempt_at is None:
                self._first_http_attempt_at = now
            self._last_http_attempt_at = now

    def record_http_status_class(self, status_code):
        if not isinstance(status_code, int):
            return
        class_name = f"{status_code // 100}xx"
        with self._lock:
            classes = self._usage.setdefault("http_status_classes", {})
            classes[class_name] = classes.get(class_name, 0) + 1

    def summary(self):
        with self._lock:
            summary = dict(self._usage)
            summary["http_status_classes"] = dict(
                self._usage.get("http_status_classes") or {}
            )
            first_attempt_at = self._first_http_attempt_at
            last_attempt_at = self._last_http_attempt_at
            execution_counts = tuple(self._nqe_execution_signatures.values())
        summary["nqe_execution_signature_count"] = len(execution_counts)
        summary["nqe_repeated_execution_count"] = sum(
            count - 1 for count in execution_counts if count > 1
        )
        summary["nqe_max_execution_signature_count"] = max(
            execution_counts,
            default=0,
        )
        summary["throttle_sleep_seconds"] = round(
            float(summary.get("throttle_sleep_seconds") or 0.0),
            6,
        )
        window_seconds = (
            max(float(last_attempt_at) - float(first_attempt_at), 0.0)
            if first_attempt_at is not None and last_attempt_at is not None
            else 0.0
        )
        http_attempts = int(summary.get("http_attempts") or 0)
        observed_rate = (
            round(((http_attempts - 1) * 60.0) / window_seconds, 3)
            if http_attempts > 1 and window_seconds > 0
            else None
        )
        summary["usage_window_seconds"] = round(window_seconds, 6)
        summary["observed_http_attempts_per_minute"] = observed_rate
        read_cache_hits = int(summary.get("read_cache_hits") or 0)
        read_cache_misses = int(summary.get("read_cache_misses") or 0)
        total_read_cache_lookups = read_cache_hits + read_cache_misses
        summary["read_cache_hit_rate"] = (
            round(read_cache_hits / float(total_read_cache_lookups), 6)
            if total_read_cache_lookups
            else None
        )
        return summary

    def reset(self):
        with self._lock:
            self._first_http_attempt_at = None
            self._last_http_attempt_at = None
            self._usage = self._empty_usage()
            self._nqe_execution_signatures = {}
