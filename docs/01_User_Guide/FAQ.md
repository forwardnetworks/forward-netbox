---
description: Frequently asked questions about the Forward Enterprise NetBox Plugin.
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

If you encounter the error message `value too long for the type character varying(64)`, it means that the string you are using is longer than the maximum value allowed by NetBox. This can occur when the `vlanName` contains more than 64 characters. The error will reference the NQE mapping entry that produced the value. You can edit the corresponding NQE query so that it truncates the value (for example `{{ object.vlanName[:64] }}`) before it is sent to NetBox.

