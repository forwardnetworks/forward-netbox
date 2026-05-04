import logging
import threading

from core.models import Job
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
            }
            cache.set(self.cache_key, self.log_data, self.cache_timeout)

    def increment_statistics(
        self, model_string: str, *, outcome: str = "applied"
    ) -> None:
        with self._lock:
            stats = self.log_data.setdefault("statistics", {}).setdefault(
                model_string,
                {
                    "current": 0,
                    "total": 0,
                    "applied": 0,
                    "failed": 0,
                    "skipped": 0,
                },
            )
            stats["current"] += 1
            if outcome in {"applied", "failed", "skipped"}:
                stats[outcome] += 1
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
                },
            )
            stats["total"] += amount
            cache.set(self.cache_key, self.log_data, self.cache_timeout)
