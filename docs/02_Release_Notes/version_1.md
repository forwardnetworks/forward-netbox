---
description: v1 Release Notes
---

# v1 Release Notes

## v1.0.11 (2023-01-09)

### Enhancements

- `SD-505` - Add option to auto merge staged changes into NetBox.
- `SD-518` - Add support for Forward tables in NetBox device UI.
- `SD-546` - Add support for NetBox Cloud Remote data collection.

### Bug Fixes

- `SD-481` - Use `nameOriginal` instead of `intName` for interface names in transform map.
- `SD-513` - Fix issue with stack member hostnames use `\<member-id>` instead of `\<member-serial>`.

## v1.0.10 (2023-10-16)

### Bug Fixes

- `SD-487` - Fix network filter when sites have not been selected in the ingestion job.

## v1.0.9 (2023-10-13)

### Bug Fixes

- `SD-475` - Support duplicate prefixes, if no prefix is provided by Forward use NetBox's Global VRF as default.
- `SD-476` - Reset ForwardSync status to `new` on form save.
- `SD-477` - Improve Jinja form field help text for transform maps.
- `SD-478` - Implement nested serializer for `ForwardBranch` and `ForwardSync` model.

## v1.0.8 (2023-10-10)

### Enhancements

- `SD-469` - Collect snapshot status.
- `SD-469` - Add padding to site labels in snapshot detail page.

### Bug Fixes

- `SA-494` - URL for the link to Forward using `sn` instead of `snHw`.
- `SD-469` - Fix snapshot names so that they show `$last` etc.
- `SD-469` - Remove parameters column from source table view.
- `SD-470` - Managed networks without a `Network` cause the ingestion to fail. Filter networks without a `Network` before ingestion.

## v1.0.7 (2023-10-04)

### Bug Fixes

- `SD-462` - `get_client` error when performing sync because argument does not have a default.

## v1.0.6 (2023-10-03)

### Enhancements

- `SD-459` - Improve wording for verify field on source form and improve logging for certificate validation failures.

## v1.0.5 (2023-09-29)

### Enhancements

- `SD-458` - Add better logging for transform map failures.

### Bug Fixes

- `SD-458` - Fix issue with default transform maps for VLANs and Interfaces not containing a `target_field` for the description field.

## v1.0.4 (2023-09-28)

- Initial Release
