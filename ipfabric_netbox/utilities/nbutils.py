from collections import Counter
from copy import deepcopy

from dcim.models import Device
from dcim.models import InventoryItem
from dcim.models import Manufacturer

DEFAULT_DEVICE_ROLE = "Network Device"
device_serial_max_length = Device._meta.get_field("serial").max_length


def order_members(members):
    devices = {}

    for member in members:
        master_serial = member.get("sn")
        if master_serial and member.get("memberSn"):
            if master_serial in devices:
                devices[master_serial].append(member)
            else:
                devices[master_serial] = [member]

    return devices


def order_devices(devices, members):
    hostnames = [d["hostname"] for d in devices]
    counter = Counter(hostnames)

    new_devices = []

    for device in devices:
        if counter[device["hostname"]] > 1:
            device["hostname"] = f"{device['hostname']} - ({device['sn']})"
        if child_members := members.get(device.get("sn")):
            for child_member in child_members:
                if device.get("sn") != child_member.get("memberSn"):
                    new_device = deepcopy(device)
                    new_device[
                        "hostname"
                    ] = f"{device['hostname']}/{child_member.get('member')}"
                    new_device["model"] = child_member.get("pn")
                    new_device["sn"] = child_member.get("memberSn")
                    new_device["virtual_chassis"] = child_member
                    new_devices.append(new_device)
                else:
                    device["virtual_chassis"] = child_member
            hostnames = [d["hostname"] for d in devices]
            counter = Counter(hostnames)

    devices.extend(new_devices)

    return devices


def create_inventory_items(device: Device, parts: list, manufacturer: Manufacturer):
    for part in parts:
        name = part.get("name", "")
        if len(name) > InventoryItem._meta.get_field("name").max_length:
            if part.get("dscr"):
                name = part.get("dscr")
            else:
                name = part.get("sn")

        defaults = {
            "name": name,
            "manufacturer": manufacturer,
            "serial": part.get("sn", ""),
            # "description": part.get('dscr', "123"),
            "part_id": part.get("pid", ""),
            "device": device,
            "lft": device.pk,
        }
        if part.get("dscr"):
            defaults["description"] = part.get("dscr")

        inventory_object, _ = InventoryItem.objects.update_or_create(
            serial=part.get("sn", ""), defaults=defaults
        )
