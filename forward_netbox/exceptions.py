from core.exceptions import SyncError


class ErrorMixin(Exception):
    model: str = ""
    defaults: dict[str, str] = {}
    coalesce_fields: dict[str, str] = {}

    def __init__(self, model: str, context: dict, data: dict = None):
        super().__init__()
        self.model = model
        self.data = data or {}
        self.defaults = context.pop("defaults", {})
        self.coalesce_fields = context


class SearchError(ErrorMixin, LookupError):
    def __str__(self):
        return f"{self.model} with these keys not found: {self.coalesce_fields}."


class SyncDataError(ErrorMixin, SyncError):
    def __str__(self):
        return f"Sync failed for {self.model}: coalesce_fields={self.coalesce_fields} defaults={self.defaults}."
