# Put back the platform a sync run blanked on a device.
#
# Before 3.0.14 an include tag that matched no device made one run read every
# compared table as gone. The platform list was emptied, and because a device's
# platform is SET_NULL (not PROTECT) NetBox let it through and cleared the field
# on every device that had one. The next normal run puts the platform back on the
# devices it covers, but a device outside the sync's scope is never revisited, so
# it stays blank.
#
# This restores exactly those, and only those: a device whose LATEST change to
# its platform is a change made by one of this sync's own ingestions that took a
# platform away, where the platform is still blank and the platform that was
# removed can be found again by its slug. NetBox's change log is the proof; a
# platform removed by anyone else, or put back and removed again by a person, is
# not touched.
#
# Preview first, like the stale-row cleanup: the preview changes nothing; the
# restore recomputes the plan and applies no more than the preview counted.
import logging
import uuid

from django.db import transaction

logger = logging.getLogger("forward_netbox.platform_restore")

PREVIEW_JOB_SUFFIX = "preview platform restore"

HOLD_PLATFORM_GONE = "platform_not_found_again"
HOLD_PLATFORM_AMBIGUOUS = "several_platforms_with_that_slug"
HOLD_CHANGED_SINCE = "platform_changed_by_someone_else_since"

REFUSED_NO_PREVIEW = "no_completed_preview"
REFUSED_GREW_SINCE_PREVIEW = "more_restorable_than_the_preview_showed"
REFUSED_SYNC_RUNNING = "a_sync_run_is_active"


class PlatformRestoreRefused(Exception):
    """The restore was asked for in a state where it must not run."""

    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


def _slug_of_removed_platform(platform_pk, cache):
    """The slug of a platform that no longer has the pk a device recorded.

    ``None`` when the platform still exists under that pk (use it as is) and
    ``""`` when nothing says what it was.
    """
    from core.models import ObjectChange
    from core.models import ObjectType
    from dcim.models import Platform

    if platform_pk in cache:
        return cache[platform_pk]
    if Platform.objects.filter(pk=platform_pk).exists():
        cache[platform_pk] = None
        return None
    change = (
        ObjectChange.objects.filter(
            changed_object_type=ObjectType.objects.get_for_model(Platform),
            changed_object_id=platform_pk,
            action="delete",
        )
        .order_by("-time")
        .first()
    )
    slug = ""
    if change is not None:
        slug = str((change.prechange_data or {}).get("slug") or "")
    cache[platform_pk] = slug
    return slug


def plan_platform_restore(sync):
    """``{"restorable": {device_pk: platform_pk}, "held": {reason: n}, ...}``.

    Counts only leave this module through `summarize_plan`.
    """
    from core.models import ObjectChange
    from core.models import ObjectType
    from dcim.models import Device
    from dcim.models import Platform

    from .routing_catalogue_cleanup import _sync_request_ids

    request_ids = set(_sync_request_ids(sync))
    plan = {"restorable": {}, "held": {}, "blanked_by_this_sync": 0}
    if not request_ids:
        return plan
    device_type = ObjectType.objects.get_for_model(Device)
    blanked = {}
    for change in (
        ObjectChange.objects.filter(
            changed_object_type=device_type,
            action="update",
            request_id__in=list(request_ids),
            prechange_data__platform__isnull=False,
            postchange_data__platform=None,
        )
        .order_by("time")
        .only("changed_object_id", "request_id", "prechange_data", "time")
        .iterator(chunk_size=2000)
    ):
        blanked[change.changed_object_id] = (
            change.time,
            (change.prechange_data or {}).get("platform"),
        )
    if not blanked:
        return plan
    still_blank = set(
        Device.objects.filter(pk__in=list(blanked), platform__isnull=True).values_list(
            "pk", flat=True
        )
    )
    plan["blanked_by_this_sync"] = len(still_blank)
    cache = {}
    for device_pk in sorted(still_blank):
        blank_time, old_platform_pk = blanked[device_pk]
        # Anything after the blanking that touched the platform means someone
        # else has had a say since: leave it alone.
        later = ObjectChange.objects.filter(
            changed_object_type=device_type,
            changed_object_id=device_pk,
            time__gt=blank_time,
        ).only("prechange_data", "postchange_data")
        if any(
            (c.prechange_data or {}).get("platform")
            != (c.postchange_data or {}).get("platform")
            for c in later
        ):
            plan["held"][HOLD_CHANGED_SINCE] = (
                plan["held"].get(HOLD_CHANGED_SINCE, 0) + 1
            )
            continue
        slug = _slug_of_removed_platform(old_platform_pk, cache)
        if slug is None:
            target = old_platform_pk
        elif not slug:
            plan["held"][HOLD_PLATFORM_GONE] = (
                plan["held"].get(HOLD_PLATFORM_GONE, 0) + 1
            )
            continue
        else:
            found = list(
                Platform.objects.filter(slug=slug).values_list("pk", flat=True)[:2]
            )
            if not found:
                plan["held"][HOLD_PLATFORM_GONE] = (
                    plan["held"].get(HOLD_PLATFORM_GONE, 0) + 1
                )
                continue
            if len(found) > 1:
                plan["held"][HOLD_PLATFORM_AMBIGUOUS] = (
                    plan["held"].get(HOLD_PLATFORM_AMBIGUOUS, 0) + 1
                )
                continue
            target = found[0]
        plan["restorable"][device_pk] = target
    return plan


def summarize_plan(plan):
    return {
        "blanked_by_this_sync": plan["blanked_by_this_sync"],
        "restorable": len(plan["restorable"]),
        "held": dict(plan["held"]),
    }


def preview_platform_restore(sync):
    """What a restore would do now. Changes nothing."""
    from django.utils import timezone

    summary = summarize_plan(plan_platform_restore(sync))
    summary["generated_at"] = timezone.now().isoformat()
    return summary


def restore_platforms(sync, *, user=None, preview_restorable=None):
    """Put the platform back. ``preview_restorable`` is the count the operator saw.

    The plan is recomputed first and nothing beyond that count is restored.
    Change-logged under ``user`` like any other edit.
    """
    from dcim.models import Device
    from dcim.models import Platform
    from netbox.context_managers import event_tracking
    from utilities.request import NetBoxFakeRequest

    if preview_restorable is None:
        raise PlatformRestoreRefused(REFUSED_NO_PREVIEW)
    plan = plan_platform_restore(sync)
    restorable = plan["restorable"]
    if len(restorable) > int(preview_restorable):
        raise PlatformRestoreRefused(REFUSED_GREW_SINCE_PREVIEW)
    platforms = {
        p.pk: p for p in Platform.objects.filter(pk__in=set(restorable.values()))
    }

    def apply():
        restored = 0
        for device in Device.objects.filter(pk__in=list(restorable)):
            platform = platforms.get(restorable[device.pk])
            if platform is None or device.platform_id is not None:
                continue
            device.snapshot()
            device.platform = platform
            device.save(update_fields=["platform"])
            restored += 1
        return restored

    if user is None:
        with transaction.atomic():
            restored = apply()
    else:
        request = NetBoxFakeRequest(
            {
                "META": {},
                "POST": {},
                "GET": {},
                "FILES": {},
                "user": user,
                "path": "",
                "id": uuid.uuid4(),
            }
        )
        with transaction.atomic(), event_tracking(request):
            restored = apply()
    summary = summarize_plan(plan)
    summary["restored"] = restored
    return summary
