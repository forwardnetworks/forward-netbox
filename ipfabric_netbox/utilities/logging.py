import logging

from django.core.cache import cache
from django.utils import timezone
from extras.choices import LogLevelChoices


class SyncLogging:
    def __init__(self, key_prefix="ipfabric_sync", job=None, cache_timeout=3600):
        self.key_prefix = key_prefix
        self.job_id = job
        self.cache_key = f"{self.key_prefix}_{job}"
        self.cache_timeout = cache_timeout
        self.log_data = {"logs": [], "statistics": {}}
        self.logger = logging.getLogger("ipfabric.sync")

    def _log(self, obj, message, level=LogLevelChoices.LOG_INFO):
        """
        Log a message from a test method. Do not call this method directly; use one of the log_* wrappers below.
        """
        if level not in LogLevelChoices.values():
            raise Exception(f"Unknown logging level: {level}")
        self.log_data["logs"].append(
            (
                timezone.now().isoformat(),
                level,
                str(obj) if obj else None,
                obj.get_absolute_url() if hasattr(obj, "get_absolute_url") else None,
                message,
            )
        )
        cache.set(self.cache_key, self.log_data, self.cache_timeout)

    def log(self, message):
        """
        Log a message which is not associated with a particular object.
        """
        self._log(None, message, level=LogLevelChoices.LOG_INFO)
        self.logger.info(message)

    def log_success(self, message, obj=None):
        """
        Record a successful test against an object.
        """
        self._log(obj, message, level=LogLevelChoices.LOG_SUCCESS)
        self.logger.info(f"Success | {obj}: {message}")

    def log_info(self, message: str, obj=None):
        """
        Log an informational message.
        """
        self._log(obj, message, level=LogLevelChoices.LOG_INFO)
        self.logger.info(f"Info | {obj}: {message}")

    def log_warning(self, message, obj=None):
        """
        Log a warning.
        """
        self._log(obj, message, level=LogLevelChoices.LOG_WARNING)
        self.logger.info(f"Warning | {obj}: {message}")

    def log_failure(self, message, obj=None):
        """
        Log a failure. Calling this method will automatically mark the report as failed.
        """
        self._log(obj, message, level=LogLevelChoices.LOG_FAILURE)
        self.logger.info(f"Failure | {obj}: {message}")

    def init_statistics(self, model: str, total: int) -> dict[str, int]:
        statistics = self.log_data.get("statistics")
        if not statistics.get(model):
            stats = statistics[model] = {"current": 0, "total": total}
        else:
            stats = statistics.get(model)
        return stats

    def increment_statistics(self, model: str, total: int = None) -> None:
        stats = self.init_statistics(model, total)
        if total:
            stats["total"] = total
        stats["current"] += 1
        cache.set(self.cache_key, self.log_data, self.cache_timeout)
        self.logger.info(
            f"{model} - {stats['current']} out of {stats['total']} processed"
        )

    def clear_log(self):
        self.log_data["logs"] = []

    @classmethod
    def retrieve_from_cache(cls, key_prefix="log"):
        cache_key = f"{key_prefix}_log"
        log_data = cache.get(cache_key)
        if log_data is None:
            return cls(key_prefix)
        log = cls(key_prefix)
        log.log_data = log_data
        return log
