---
description: Frequently asked questions about the Forward NetBox Plugin.
---

# FAQ

## Information

### NetBox API

Currently, in `v1.0.0`, we have only implemented the read-only endpoints. Writable API endpoints will be added in the future.

## Errors and Warnings

### Branch Timeout Error

If you get the error message `Branch Failed: Task exceeded maximum timeout value (300 seconds)`, it is due to the default timeout value for the background task being exceeded. This can be resolved by increasing the timeout value for the background task. This can be done by setting the `RQ_DEFAULT_TIMEOUT` value in the NetBox `configuration.py` file. The default value is `300` seconds.

### CERTIFICATE_VERIFY_FAILED

If you get the error message `CERTIFICATE_VERIFY_FAILED`, it is due to the certificate not being trusted. This can be resolved by setting the `Verify` field to `false` when creating the source. This will disable the validation of the certificate. Currently, there is no way to validate custom certificates; we are working on that, but disabling the validation is a temporary way to get around this issue.

### VLAN Name Too Long

If you encounter the error message `value too long for the type character varying(64)`, it means that the string you are using is longer than the maximum value allowed by NetBox. This can occur when the `vlanName` contains more than 64 characters. The error message will display the name of the transform map. You can edit it and restrict the input to the necessary number of characters using `{{ object.vlanName[:64] }}`.


<!-- vale off -->
### Cannot resolve bases for [<ModelState: 'forward_netbox.ForwardBranch'\>]
<!-- vale on -->

This error message appear if you were running `forward_netbox` plugin with version <4.0.0 and upgraded to NetBox v4.3.0+.

The root cause is removed model in NetBox v4.3.0 that is used as parent of `ForwardBranch`.

To resolve this issue, you need to completely purge plugin from your database and proceed with clean installation. Following steps should help you:

1. Remove plugin from `PLUGINS` list in `configuration.py`.
2. Get to database shell:
   1. If you're running NetBox on bare metal or VM:
      ```bash
      sudo -u netbox /opt/netbox/venv/bin/python3 /opt/netbox/netbox/manage.py dbshell
      ```
   2. If you're running NetBox in Docker:
      ```bash
      PSQL_CONTAINER_NAME = "netbox_postgres_1"
      docker exec -it $PSQL_CONTAINER_NAME psql -U netbox -h localhost
      ```
3. Drop all tables related to forward_netbox plugin including migration records:
   ```sql
   DROP TABLE IF EXISTS
       forward_netbox_forwardbranch,
       forward_netbox_forwarddata,
       forward_netbox_forwardingestion,
       forward_netbox_forwardrelationshipfield,
       forward_netbox_forwardsnapshot,
       forward_netbox_forwardsource,
       forward_netbox_forwardsync,
       forward_netbox_forwardtransformfield,
       forward_netbox_forwardtransformmap
   CASCADE;
   DELETE FROM django_migrations WHERE app = 'forward_netbox';
   ```
4. Re-add plugin to `PLUGINS` list in `configuration.py`.
5. Run migrations.
