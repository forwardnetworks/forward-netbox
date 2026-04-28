# Device Type Alias Data File

The default device type maps do not require a Forward data file. They remain the safe default and continue to emit Forward model strings directly.

For environments that pre-load NetBox device types from the NetBox Device Type Library, the plugin also seeds two disabled alternate NQE maps:

- `Forward Device Models with NetBox Device Type Aliases`
- `Forward Devices with NetBox Device Type Aliases`

These alternates use a Forward NQE data file to map collected Forward model strings, such as part numbers, to canonical NetBox library `model` and `slug` values. The same file also carries Forward vendor to NetBox manufacturer override rows for the alias-aware maps, keeping site-specific data separate from query logic.

## Required Forward Data File

Create a Forward data file with:

- File name: `netbox_device_type_aliases.json`
- NQE name: `netbox_device_type_aliases`
- File type: `JSON`

Attach the data file to the Forward network used by the sync. Forward's interactive inventory-query/report paths can evaluate against latest data files immediately after upload. The public `/api/nqe` path used by the plugin runs against the selected snapshot, so the selected sync snapshot must include the uploaded data file value before aliases affect plugin results.

If this data file is not uploaded and attached, do not enable the alias-aware NQE maps. Forward's NQE schema only exposes data files that exist for the network, so a query that references a missing data file will fail validation. The default maps remain the fallback.

If the data-file field exists but the selected snapshot has no attached value, the alias-aware maps fall back to the raw Forward model strings instead of returning zero rows. This keeps the optional path safe, but it also means no Device Type Library aliases are applied until Forward exposes rows in `network.extensions.netbox_device_type_aliases.value`.

## Build The JSON

Clone or update the NetBox Device Type Library, then run:

```bash
python scripts/build_netbox_device_type_aliases.py \
  /path/to/devicetype-library \
  --output netbox_device_type_aliases.json \
  --conflicts-output netbox_device_type_alias_conflicts.json
```

The script requires PyYAML:

```bash
python -m pip install pyyaml
```

The generated JSON is a flat array with a stable schema. Rows use `record_type` to distinguish device type aliases from manufacturer overrides:

```json
[
  {
    "record_type": "manufacturer_override",
    "forward_vendor": "Vendor.CISCO",
    "forward_manufacturer": "Cisco",
    "forward_manufacturer_slug": "cisco",
    "manufacturer": "Cisco",
    "manufacturer_slug": "cisco",
    "match_source": "manufacturer_override",
    "source": "forward-netbox",
    "source_commit": "",
    "source_path": "",
    "forward_model": "",
    "forward_model_slug": "",
    "netbox_model": "",
    "netbox_slug": "",
    "part_number": ""
  },
  {
    "record_type": "device_type_alias",
    "forward_vendor": "",
    "forward_manufacturer": "Cisco",
    "forward_manufacturer_slug": "cisco",
    "forward_model": "ISR4331/K9",
    "forward_model_slug": "isr4331-slash-k9",
    "manufacturer": "Cisco",
    "manufacturer_slug": "cisco",
    "netbox_model": "ISR4331",
    "netbox_slug": "cisco-isr4331",
    "part_number": "ISR4331/K9",
    "match_source": "part_number",
    "source": "netbox-community/devicetype-library",
    "source_commit": "example",
    "source_path": "device-types/Cisco/ISR4331.yaml"
  }
]
```

Exact model aliases take precedence over part-number aliases. Conflicting derived aliases are skipped and written to the optional conflicts report. The default generated file includes manufacturer override rows equivalent to the shipped NQE helper table; pass `--no-manufacturer-overrides` only if you intentionally want the alias-aware queries to use their static fallback helper for manufacturer names.

## Enable In NetBox

1. Upload and attach `netbox_device_type_aliases.json` in Forward.
2. In NetBox, open `Plugins > Forward Networks > NQE Maps`.
3. Disable `Forward Device Models` and enable `Forward Device Models with NetBox Device Type Aliases`.
4. Disable `Forward Devices` and enable `Forward Devices with NetBox Device Type Aliases`.
5. For large datasets, copy the alias-aware query text into the Forward Org Repository and configure the NetBox NQE maps to use the committed query IDs.

Only switch both maps together. The device query and device model query should agree on the same `device_type` and `device_type_slug` values.
