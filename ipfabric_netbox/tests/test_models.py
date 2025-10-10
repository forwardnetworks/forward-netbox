import copy
import json

from dcim.models import Device
from django.contrib.contenttypes.models import ContentType
from django.core import serializers
from django.test import TestCase
from django.utils import timezone

from ipfabric_netbox.choices import IPFabricSnapshotStatusModelChoices
from ipfabric_netbox.models import IPFabricIngestion
from ipfabric_netbox.models import IPFabricSnapshot
from ipfabric_netbox.models import IPFabricSource
from ipfabric_netbox.models import IPFabricSync
from ipfabric_netbox.models import IPFabricTransformField
from ipfabric_netbox.models import IPFabricTransformMap
from ipfabric_netbox.utilities.ipfutils import IPFabricSyncRunner


class IPFabricTransformMapModelTestCase(TestCase):
    def setUp(self):
        source = IPFabricSource.objects.create(
            name="test",
            url="https://localhost",
            status="new",
            parameters={"auth": "token123", "verify": True},
        )
        snapshot = IPFabricSnapshot.objects.create(
            name="S01 - Day 2 - IPF Lab - 02-Jul-21 06:29:16 - 12dd8c61-129c-431a-b98b-4c9211571f89",
            source=source,
            snapshot_id="12dd8c61-129c-431a-b98b-4c9211571f89",
            status=IPFabricSnapshotStatusModelChoices.STATUS_LOADED,
            data={
                "end": "2021-07-02T06:29:16.311000Z",
                "name": "S01 - Day 2 - IPF Lab",
                "note": "Multi-Environment containing:\\nAWS, Azure, ACI, NSX-T, Viptela, Versa, SilverPeak, Meraki\\n\\nSite 48 - devices added, NTP issue\\nSite 68 &38 - NTP update\\nSite 38 - resiliency affected (no ospfx2 - no L1 link x1) + passive interfaces FIXED / NTP partial update\\n?E2E: 38 - 66 - migration HTTP to HTTPS\\n?Site 66 - FW bypass E2E\\nVRRP improvements (LAB1 / L52)",
                "sites": [
                    "35COLO",
                    "35HEADOFFICE",
                    "35PRODUCTION",
                    "35SALES",
                    "38 Pilsen DR",
                    "66 Ostrava DC",
                    "68 Pardubice Distribution",
                    "ACI",
                    "AWS_SITE",
                    "AZURE",
                    "HWLAB",
                    "L31",
                    "L33",
                    "L34",
                    "L35",
                    "L36",
                    "L37",
                    "L39",
                    "L43",
                    "L45",
                    "L46",
                    "L47",
                    "L48",
                    "L49",
                    "L51",
                    "L52",
                    "L62",
                    "L63",
                    "L64",
                    "L65",
                    "L67",
                    "L71",
                    "L72",
                    "L77",
                    "L81",
                    "LAB01",
                    "MERAKI_SITE",
                    "MPLS",
                    "NSX-T",
                    "SILVERPEAK",
                    "VERSA_SITE",
                    "VIPTELA",
                ],
                "start": "2021-07-02T06:00:00.930000Z",
                "change": "2022-03-25T14:35:48.277000Z",
                "errors": [
                    {"count": 2, "error_type": "ABMapResultError"},
                    {"count": 5, "error_type": "ABParseError"},
                    {"count": 3, "error_type": "ABTaskMapResultError"},
                    {"count": 1, "error_type": "ABAmbiguousCommand"},
                    {"count": 7, "error_type": "ABCmdAuthFail"},
                    {"count": 6, "error_type": "ABCommandTimeout"},
                    {"count": 1, "error_type": "ABNoConfig"},
                    {"count": 1, "error_type": "ABParseBadConfigError"},
                    {"count": 1, "error_type": "ABTaskMapResultBadConfigError"},
                    {"count": 1, "error_type": "ABWorkerAuthError"},
                ],
                "locked": False,
                "status": "loaded",
                "loading": False,
                "version": "6.3.0-13",
                "user_count": 2324,
                "loaded_size": 170856074,
                "snapshot_id": "12dd8c61-129c-431a-b98b-4c9211571f89",
                "from_archive": True,
                "finish_status": "loaded",
                "unloaded_size": 26914884,
                "initial_version": "4.4.3+2",
                "interface_count": 9608,
                "total_dev_count": 729,
                "creator_username": None,
                "device_added_count": 0,
                "licensed_dev_count": 720,
                "device_removed_count": 0,
                "disabled_graph_cache": False,
                "interface_edge_count": 534,
                "interface_active_count": 6379,
                "disabled_historical_data": False,
                "disabled_intent_verification": False,
            },
            last_updated=timezone.now(),
        )
        sync = IPFabricSync.objects.create(
            name="ingest",
            status="new",
            snapshot_data=snapshot,
            update_custom_fields=True,
            parameters={
                "vrf": False,
                "site": True,
                "vlan": False,
                "sites": [],
                "device": True,
                "prefix": False,
                "platform": True,
                "interface": False,
                "ipaddress": False,
                "devicerole": True,
                "devicetype": True,
                "manufacturer": True,
                "virtualchassis": False,
            },
        )

        ingestion = IPFabricIngestion.objects.create(sync=sync)

        runner = IPFabricSyncRunner(
            settings={
                "site": True,
                "sites": [],
                "device": True,
                "platform": True,
                "interface": False,
                "devicerole": True,
                "devicetype": True,
                "manufacturer": True,
                "virtualchassis": True,
                "snapshot_id": "12dd8c61-129c-431a-b98b-4c9211571f89",
            },
            sync=sync,
            ingestion=ingestion,
        )
        # Need to monkeypatch since we are not in active Branch (different schema)
        # Using default schema "default" here since we are in "test_netbox" DB
        runner.get_db_connection_name = lambda: "default"

        site_data = {
            "siteName": "MPLS",
            "devicesCount": 1,
            "usersCount": 2,
            "stpDCount": 0,
            "switchesCount": 0,
            "vlanCount": 1,
            "rDCount": 0,
            "routersCount": 0,
            "networksCount": 6,
        }

        self.site = runner.sync_item(
            item=site_data, app_label="dcim", model="site", cf=sync.update_custom_fields
        )

        device_data = {
            "id": "961251111",
            "configReg": "0x0",
            "devType": "router",
            "family": "ios",
            "hostname": "L21PE152",
            "hostnameOriginal": None,
            "hostnameProcessed": None,
            "domain": None,
            "fqdn": None,
            "icon": None,
            "image": "unix:/opt/unetlab/addons/iol/bin/i86bi-linux-l3-adventerprisek9-15.2",
            "objectId": None,
            "taskKey": "fb67e3b4-5e48-4e52-b000-56cb187f2852",
            "loginIp": "10.21.254.152",
            "loginType": "telnet",
            "loginPort": None,
            "mac": None,
            "memoryTotalBytes": 396008048,
            "memoryUsedBytes": 72264172,
            "memoryUtilization": 18.25,
            "model": "",
            "platform": "i86bi_linux",
            "processor": "Intel-x86",
            "rd": "3",
            "reload": "reload at 0",
            "siteName": "MPLS",
            "sn": "a15ff98",
            "snHw": "a15ff98",
            "stpDomain": None,
            "uptime": 7254180,
            "vendor": "cisco",
            "version": "15.2(4)M1",
            "slug": None,
        }

        self.mf_obj = runner.sync_item(
            item=device_data,
            app_label="dcim",
            model="manufacturer",
            cf=sync.update_custom_fields,
        )
        self.dt_obj = runner.sync_item(
            item=device_data,
            app_label="dcim",
            model="devicetype",
            cf=sync.update_custom_fields,
        )
        self.platform = runner.sync_item(
            item=device_data,
            app_label="dcim",
            model="platform",
            cf=sync.update_custom_fields,
        )
        self.role = runner.sync_item(
            item=device_data,
            app_label="dcim",
            model="devicerole",
            cf=sync.update_custom_fields,
        )
        self.device_object = runner.sync_item(
            item=device_data,
            app_label="dcim",
            model="device",
            cf=sync.update_custom_fields,
        )

    def test_transform_map(self):
        site_transform_map = IPFabricTransformMap.objects.get(name="Site Transform Map")
        self.assertEqual(site_transform_map.name, "Site Transform Map")
        self.assertEqual(site_transform_map.source_model, "site")
        self.assertEqual(
            site_transform_map.target_model,
            ContentType.objects.filter(app_label="dcim", model="site")[0],
        )

    def test_transform_field(self):
        site_transform_map = IPFabricTransformMap.objects.get(name="Site Transform Map")
        site_slug_field = IPFabricTransformField.objects.get(
            source_field="siteName",
            target_field="slug",
            transform_map=site_transform_map,
        )
        self.assertEqual(site_slug_field.source_field, "siteName")
        self.assertEqual(site_slug_field.target_field, "slug")
        self.assertEqual(site_slug_field.template, "{{ object.siteName | slugify }}")
        self.assertEqual(site_slug_field.transform_map, site_transform_map)
        site_name_field = IPFabricTransformField.objects.get(
            source_field="siteName",
            target_field="name",
            transform_map=site_transform_map,
        )
        self.assertEqual(site_name_field.source_field, "siteName")
        self.assertEqual(site_name_field.target_field, "name")
        self.assertEqual(site_name_field.template, "")
        self.assertEqual(site_name_field.transform_map, site_transform_map)

    def test_transform_map_serialization(self):
        site_transform_map = IPFabricTransformMap.objects.get(name="Site Transform Map")
        data = serializers.serialize("json", [site_transform_map])
        data = json.loads(data)[0]
        test_data = {
            "model": "ipfabric_netbox.ipfabrictransformmap",
            "pk": site_transform_map.pk,
            "fields": {
                "name": "Site Transform Map",
                "source_model": "site",
                "target_model": ContentType.objects.get(
                    app_label="dcim", model="site"
                ).pk,
            },
        }
        new_data = copy.deepcopy(data)
        new_data.pop("fields")
        new_fields = {}
        for k in test_data["fields"]:
            new_fields[k] = data["fields"][k]
        new_data["fields"] = new_fields
        self.assertDictEqual(test_data, new_data)

    def test_transform_field_serialization(self):
        site_transform_map = IPFabricTransformMap.objects.get(name="Site Transform Map")
        site_slug_field = IPFabricTransformField.objects.get(
            source_field="siteName", target_field="slug"
        )
        data = serializers.serialize("json", [site_slug_field])
        data = json.loads(data)[0]
        test_data = {
            "model": "ipfabric_netbox.ipfabrictransformfield",
            "pk": site_slug_field.pk,
            "fields": {
                "source_field": "siteName",
                "target_field": "slug",
                "template": "{{ object.siteName | slugify }}",
                "transform_map": site_transform_map.pk,
            },
        }
        new_data = copy.deepcopy(data)
        new_data.pop("fields")
        new_data["fields"] = {k: data["fields"][k] for k in test_data["fields"]}
        self.assertDictEqual(test_data, new_data)

    def test_update_or_create_instance_site(self):
        site_transform_map = IPFabricTransformMap.objects.get(name="Site Transform Map")
        data = {
            "siteName": "Site 1",
            "devicesCount": 1,
            "usersCount": 2,
            "stpDCount": 0,
            "switchesCount": 0,
            "vlanCount": 1,
            "rDCount": 0,
            "routersCount": 0,
            "networksCount": 6,
        }
        context = site_transform_map.get_context(data)
        object = site_transform_map.update_or_create_instance(context)
        self.assertEqual(object.name, "Site 1")
        self.assertEqual(object.slug, "site-1")

    def test_update_or_create_instance_device(self):
        device_object = Device.objects.first()

        self.assertEqual(device_object.name, "L21PE152")
        self.assertEqual(device_object.serial, "a15ff98")
        self.assertEqual(device_object.platform, self.platform)
        self.assertEqual(device_object.role, self.role)
        self.assertEqual(device_object.device_type, self.dt_obj)
        self.assertEqual(device_object.device_type.manufacturer, self.mf_obj)
        self.assertEqual(device_object.site, self.site)
        self.assertEqual(device_object.status, "active")

    def test_alter_transform_field_template(self):
        sync = IPFabricSync.objects.get(name="ingest")

        runner = IPFabricSyncRunner(
            settings={
                "site": True,
                "sites": [],
                "device": True,
                "platform": True,
                "interface": False,
                "devicerole": True,
                "devicetype": True,
                "manufacturer": True,
                "virtualchassis": True,
                "snapshot_id": "12dd8c61-129c-431a-b98b-4c9211571f89",
            },
            sync=sync,
        )

        device_data = {
            "id": "961251111",
            "configReg": "0x0",
            "devType": "router",
            "family": "ios",
            "hostname": "L21PE152",
            "hostnameOriginal": None,
            "hostnameProcessed": None,
            "domain": None,
            "fqdn": None,
            "icon": None,
            "image": "unix:/opt/unetlab/addons/iol/bin/i86bi-linux-l3-adventerprisek9-15.2",
            "objectId": None,
            "taskKey": "fb67e3b4-5e48-4e52-b000-56cb187f2852",
            "loginIp": "10.21.254.152",
            "loginType": "telnet",
            "loginPort": None,
            "mac": None,
            "memoryTotalBytes": 396008048,
            "memoryUsedBytes": 72264172,
            "memoryUtilization": 18.25,
            "model": "",
            "platform": "i86bi_linux",
            "processor": "Intel-x86",
            "rd": "3",
            "reload": "reload at 0",
            "siteName": "MPLS",
            "sn": "a15ff98",
            "snHw": "a15ff98",
            "stpDomain": None,
            "uptime": 7254180,
            "vendor": "cisco",
            "version": "15.2(4)M1",
            "slug": None,
        }

        transform_field = IPFabricTransformField.objects.get(
            source_field="hostname",
            target_field="name",
            transform_map=IPFabricTransformMap.objects.get(
                source_model="device",
                target_model=ContentType.objects.get(app_label="dcim", model="device"),
            ),
        )
        transform_field.template = "{{ object.hostname }} - test"
        transform_field.save()
        device_object = runner.sync_item(
            item=device_data,
            app_label="dcim",
            model="device",
            cf=sync.update_custom_fields,
        )
        self.assertEqual(device_object.name, "L21PE152 - test")
