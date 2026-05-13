from __future__ import annotations

from datetime import date
from datetime import datetime
from datetime import time
from decimal import Decimal
from uuid import UUID

from django.db.models import Model


def json_safe_value(value):
    if isinstance(value, Model):
        return {
            "model": value._meta.label_lower,
            "pk": value.pk,
            "display": str(value),
        }
    if isinstance(value, dict):
        return {str(key): json_safe_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe_value(item) for item in value]
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, (UUID, Decimal)):
        return str(value)
    return value
