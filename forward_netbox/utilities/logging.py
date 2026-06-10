import logging
import threading

from core.models import Job
from django.contrib.contenttypes.models import ContentType
from django.core.cache import cache
from django.utils import timezone
from extras.choices import LogLevelChoices


class SyncLogging:
    def __init__(self, key_prefix="forward_sync", job=None, cache_timeout=3600):
        self.key_prefix = key_prefix
        self.job_id = job
        self.cache_key = f"{self.key_prefix}_{job}"
        self.cache_timeout = cache_timeout
        self.log_data = {"logs": [], "statistics": {}}
        self.logger = logging.getLogger("forward_netbox.sync")
        self._lock = threading.RLock()

    def __getstate__(self):
        state = self.__dict__.copy()
        del state["_lock"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._lock = threading.RLock()

    def _log(self, obj, message, level=LogLevelChoices.LOG_INFO):
        timestamp = timezone.now()
        entry = (
            timestamp.isoformat(),
            level,
            str(obj) if obj else None,
            obj.get_absolute_url() if hasattr(obj, "get_absolute_url") else None,
            message,
        )
        job_entry = {
            "timestamp": timestamp,
            "level": self._normalize_job_level(level),
            "message": message,
        }
        with self._lock:
            self.log_data["logs"].append(entry)
            cache.set(self.cache_key, self.log_data, self.cache_timeout)
            self._persist_core_job_entry(job_entry)

    @staticmethod
    def _normalize_job_level(level):
        return {
            LogLevelChoices.LOG_SUCCESS: "info",
            LogLevelChoices.LOG_FAILURE: "error",
        }.get(level, level)

    def _persist_core_job_entry(self, entry):
        if not self.job_id:
            return
        try:
            job = Job.objects.get(pk=self.job_id)
        except Job.DoesNotExist:
            return
        except Exception as exc:
            self.logger.warning(
                "Failed to load core job %s for log persistence: %s", self.job_id, exc
            )
            return

        object_type_id = getattr(job, "object_type_id", None)
        if (
            object_type_id
            and not ContentType.objects.filter(pk=object_type_id).exists()
        ):
            self.logger.warning(
                "Skipping core job log persistence for job %s because content type %s is missing.",
                self.job_id,
                object_type_id,
            )
            return

        log_entries = list(job.log_entries or [])
        log_entries.append(entry)
        job.log_entries = log_entries
        try:
            job.save(update_fields=["log_entries"])
        except Exception as exc:
            self.logger.warning(
                "Failed to persist core job log entry for job %s: %s",
                self.job_id,
                exc,
            )

    def log_success(self, message, obj=None):
        self._log(obj, message, level=LogLevelChoices.LOG_SUCCESS)
        self.logger.info("Success | %s: %s", obj, message)

    def log_info(self, message, obj=None):
        self._log(obj, message, level=LogLevelChoices.LOG_INFO)
        self.logger.info("Info | %s: %s", obj, message)

    def log_warning(self, message, obj=None):
        self._log(obj, message, level=LogLevelChoices.LOG_WARNING)
        self.logger.warning("Warning | %s: %s", obj, message)

    def log_failure(self, message, obj=None):
        self._log(obj, message, level=LogLevelChoices.LOG_FAILURE)
        self.logger.error("Failure | %s: %s", obj, message)

    def init_statistics(self, model_string: str, total: int) -> None:
        with self._lock:
            self.log_data.setdefault("statistics", {})[model_string] = {
                "current": 0,
                "total": total,
                "applied": 0,
                "failed": 0,
                "skipped": 0,
                "unchanged": 0,
            }
            cache.set(self.cache_key, self.log_data, self.cache_timeout)

    def increment_statistics(
        self, model_string: str, *, outcome: str = "applied", amount: int = 1
    ) -> None:
        amount = max(0, int(amount or 0))
        if amount <= 0:
            return
        with self._lock:
            stats = self.log_data.setdefault("statistics", {}).setdefault(
                model_string,
                {
                    "current": 0,
                    "total": 0,
                    "applied": 0,
                    "failed": 0,
                    "skipped": 0,
                    "unchanged": 0,
                },
            )
            stats["current"] += amount
            if outcome in {"applied", "failed", "skipped", "unchanged"}:
                stats[outcome] += amount
            cache.set(self.cache_key, self.log_data, self.cache_timeout)

    def add_statistics_total(self, model_string: str, amount: int) -> None:
        if amount <= 0:
            return
        with self._lock:
            stats = self.log_data.setdefault("statistics", {}).setdefault(
                model_string,
                {
                    "current": 0,
                    "total": 0,
                    "applied": 0,
                    "failed": 0,
                    "skipped": 0,
                    "unchanged": 0,
                },
            )
            stats["total"] += amount
            cache.set(self.cache_key, self.log_data, self.cache_timeout)

    def set_api_usage_summary(self, summary: dict) -> None:
        with self._lock:
            self.log_data["forward_api_usage"] = dict(summary or {})
            cache.set(self.cache_key, self.log_data, self.cache_timeout)

    def add_dependency_lookup_summary(self, summary: dict) -> None:
        summary = dict(summary or {})
        if not summary:
            return
        model_string = str(summary.get("model") or "").strip()
        if not model_string:
            return
        with self._lock:
            bucket = self.log_data.setdefault(
                "dependency_lookup_cache",
                {
                    "available": False,
                    "row_count": 0,
                    "primed_target_count": 0,
                    "model_count": 0,
                    "models": [],
                },
            )
            bucket["available"] = True
            bucket["row_count"] = int(bucket.get("row_count") or 0) + int(
                summary.get("row_count") or 0
            )
            bucket["primed_target_count"] = int(
                bucket.get("primed_target_count") or 0
            ) + int(summary.get("primed_target_count") or 0)
            models = list(bucket.get("models") or [])
            existing = next(
                (
                    item
                    for item in models
                    if str(item.get("model") or "") == model_string
                ),
                None,
            )
            if existing is None:
                models.append(summary)
            else:
                existing["row_count"] = int(existing.get("row_count") or 0) + int(
                    summary.get("row_count") or 0
                )
                existing["primed_target_count"] = int(
                    existing.get("primed_target_count") or 0
                ) + int(summary.get("primed_target_count") or 0)
                for key in (
                    "device_name_count",
                    "tag_row_count",
                    "interface_pair_count",
                    "module_bay_pair_count",
                    "fhrp_group_count",
                    "ipam_identity_row_count",
                    "ipam_global_host_row_count",
                ):
                    existing[key] = int(existing.get(key) or 0) + int(
                        summary.get(key) or 0
                    )
            bucket["model_count"] = len(models)
            bucket["models"] = sorted(
                models,
                key=lambda item: (
                    -int(item.get("row_count") or 0),
                    str(item.get("model") or ""),
                ),
            )[:10]
            cache.set(self.cache_key, self.log_data, self.cache_timeout)

    def add_dependency_parent_coverage_summary(self, summary: dict) -> None:
        summary = dict(summary or {})
        if not summary or not summary.get("available"):
            return
        model_string = str(summary.get("model") or "").strip()
        if not model_string:
            return
        with self._lock:
            bucket = self.log_data.setdefault(
                "dependency_parent_coverage",
                {
                    "available": False,
                    "row_count": 0,
                    "blocked_row_count": 0,
                    "missing_parent_count": 0,
                    "model_count": 0,
                    "models": [],
                },
            )
            bucket["available"] = True
            bucket["row_count"] = int(bucket.get("row_count") or 0) + int(
                summary.get("row_count") or 0
            )
            bucket["blocked_row_count"] = int(
                bucket.get("blocked_row_count") or 0
            ) + int(summary.get("blocked_row_count") or 0)
            bucket["missing_parent_count"] = int(
                bucket.get("missing_parent_count") or 0
            ) + int(summary.get("missing_parent_count") or 0)
            models = list(bucket.get("models") or [])
            existing = next(
                (
                    item
                    for item in models
                    if str(item.get("model") or "") == model_string
                ),
                None,
            )
            if existing is None:
                models.append(summary)
            else:
                existing["row_count"] = int(existing.get("row_count") or 0) + int(
                    summary.get("row_count") or 0
                )
                existing["blocked_row_count"] = int(
                    existing.get("blocked_row_count") or 0
                ) + int(summary.get("blocked_row_count") or 0)
                existing["missing_parent_count"] = int(
                    existing.get("missing_parent_count") or 0
                ) + int(summary.get("missing_parent_count") or 0)
                missing_names = set(existing.get("missing_parent_names") or [])
                missing_names.update(summary.get("missing_parent_names") or [])
                existing["missing_parent_names"] = sorted(missing_names)
                existing_groups = list(existing.get("groups") or [])
                existing_groups.extend(summary.get("groups") or [])
                existing["groups"] = existing_groups
            bucket["model_count"] = len(models)
            bucket["models"] = sorted(
                models,
                key=lambda item: (
                    -int(item.get("blocked_row_count") or 0),
                    -int(item.get("row_count") or 0),
                    str(item.get("model") or ""),
                ),
            )[:10]
            cache.set(self.cache_key, self.log_data, self.cache_timeout)
