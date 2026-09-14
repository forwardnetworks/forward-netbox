# Generated for 2.9.6: an operator deleting a device on main now releases
# its ForwardDeviceIdentity and ForwardDeviceTagClaim rows instead of being
# refused by them; every engine path (sync, merge, prune, fast baseline)
# still gets PROTECT. See release_on_operator_delete in forward_netbox/models.py.
from django.db import migrations
from django.db import models

import forward_netbox.models


class Migration(migrations.Migration):

    dependencies = [
        ("dcim", "0001_initial"),
        ("forward_netbox", "0054_aci_tenant_policy_nqe_map_choices"),
    ]

    operations = [
        migrations.AlterField(
            model_name="forwarddeviceidentity",
            name="device",
            field=models.ForeignKey(
                on_delete=forward_netbox.models.release_on_operator_delete,
                related_name="+",
                to="dcim.device",
            ),
        ),
        migrations.AlterField(
            model_name="forwarddevicetagclaim",
            name="device",
            field=models.ForeignKey(
                on_delete=forward_netbox.models.release_on_operator_delete,
                related_name="+",
                to="dcim.device",
            ),
        ),
    ]
