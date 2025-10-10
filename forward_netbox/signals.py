import logging

from dcim.models import Device
from ipam.models import IPAddress
from netbox_branching.contextvars import active_branch

logger = logging.getLogger("forward_netbox.utilities.fwdutils")


def clear_other_primary_ip(instance: Device, **kwargs) -> None:
    """
    When a new device is created with primary IP, make sure there is no other device with the same IP.

    This signal is used when merging stashed changes. It's needed because we cannot
    guarantee that removing primary IP from Device will happen before adding new one.
    """
    try:
        if not instance.primary_ip:
            # The device has no primary IP, nothing to do
            return
    except IPAddress.DoesNotExist:
        # THe IP is not created yet, cannot be assigned
        return
    try:
        connection_name = None
        if branch := active_branch.get():
            connection_name = branch.connection_name
        other_device = Device.objects.using(connection_name).get(
            primary_ip4=instance.primary_ip
        )
        if other_device and instance != other_device:
            other_device.snapshot()
            other_device.primary_ip4 = None
            other_device.save(using=connection_name)
    except Device.DoesNotExist:
        pass

