from django.core.exceptions import ObjectDoesNotExist

from ..exceptions import ForwardDependencySkipError
from ..exceptions import ForwardSyncDataError


def lookup_cable_between(runner, interface, remote_interface):
    interface.refresh_from_db(fields=["cable"])
    remote_interface.refresh_from_db(fields=["cable"])
    if interface.cable_id and interface.cable_id == remote_interface.cable_id:
        return interface.cable
    return None


def delete_dcim_cable(runner, row):
    from dcim.models import Device

    device = Device.objects.filter(name=row.get("device")).order_by("pk").first()
    remote_device = (
        Device.objects.filter(name=row.get("remote_device")).order_by("pk").first()
    )
    if device is None or remote_device is None:
        return False
    interface = runner._lookup_interface(device, row.get("interface"))
    remote_interface = runner._lookup_interface(
        remote_device, row.get("remote_interface")
    )
    if interface is None or remote_interface is None:
        return False
    cable = lookup_cable_between(runner, interface, remote_interface)
    if cable is None:
        return False
    cable.delete()
    return True


def apply_dcim_cable(runner, row):
    from dcim.models import Cable
    from dcim.models import Device

    try:
        device = Device.objects.get(name=row["device"])
    except ObjectDoesNotExist as exc:
        key = (row["device"],)
        if runner._dependency_failed("dcim.device", key):
            raise ForwardDependencySkipError(
                f"Skipping cable because dependency `dcim.device` failed for {key}.",
                model_string="dcim.cable",
                context={
                    "device": row["device"],
                    "interface": row.get("interface"),
                },
                data=row,
            ) from exc
        runner.logger.log_warning(
            f"Skipping cable row because device `{row['device']}` was not found.",
            obj=runner.sync,
        )
        return False

    try:
        remote_device = Device.objects.get(name=row["remote_device"])
    except ObjectDoesNotExist as exc:
        key = (row["remote_device"],)
        if runner._dependency_failed("dcim.device", key):
            raise ForwardDependencySkipError(
                f"Skipping cable because dependency `dcim.device` failed for {key}.",
                model_string="dcim.cable",
                context={
                    "device": row["remote_device"],
                    "interface": row.get("remote_interface"),
                },
                data=row,
            ) from exc
        runner.logger.log_warning(
            f"Skipping cable row because remote device `{row['remote_device']}` was not found.",
            obj=runner.sync,
        )
        return False

    interface = runner._lookup_interface(device, row["interface"])
    if interface is None:
        key = (device.name, row["interface"])
        if runner._dependency_failed("dcim.interface", key):
            raise ForwardDependencySkipError(
                f"Skipping cable because dependency `dcim.interface` failed for {key}.",
                model_string="dcim.cable",
                context={"device": device.name, "interface": row["interface"]},
                data=row,
            )
        runner.logger.log_warning(
            f"Skipping cable row because interface `{row['interface']}` was not found on `{device.name}`.",
            obj=runner.sync,
        )
        return False

    remote_interface = runner._lookup_interface(remote_device, row["remote_interface"])
    if remote_interface is None:
        key = (remote_device.name, row["remote_interface"])
        if runner._dependency_failed("dcim.interface", key):
            raise ForwardDependencySkipError(
                f"Skipping cable because dependency `dcim.interface` failed for {key}.",
                model_string="dcim.cable",
                context={
                    "device": remote_device.name,
                    "interface": row["remote_interface"],
                },
                data=row,
            )
        runner.logger.log_warning(
            f"Skipping cable row because interface `{row['remote_interface']}` was not found on `{remote_device.name}`.",
            obj=runner.sync,
        )
        return False

    cable = lookup_cable_between(runner, interface, remote_interface)
    if cable is not None:
        cable.status = row["status"]
        cable.full_clean()
        cable.save()
        return

    interface.refresh_from_db(fields=["cable"])
    remote_interface.refresh_from_db(fields=["cable"])
    if interface.cable_id or remote_interface.cable_id:
        if runner._conflict_policy("dcim.cable") == "skip_warn_aggregate":
            runner._record_aggregated_conflict_warning(
                model_string="dcim.cable",
                reason="interface-already-cabled",
                warning_message=(
                    "Skipping cable row because one or both interfaces are already connected to a different cable."
                ),
            )
            return False
        raise ForwardSyncDataError(
            "Unable to create cable because one or both interfaces are already connected to a different cable.",
            model_string="dcim.cable",
            context={
                "device": row.get("device"),
                "interface": row.get("interface"),
                "remote_device": row.get("remote_device"),
                "remote_interface": row.get("remote_interface"),
            },
            data=row,
        )

    cable = Cable(
        a_terminations=[interface],
        b_terminations=[remote_interface],
        status=row["status"],
    )
    cable.full_clean()
    cable.save()
