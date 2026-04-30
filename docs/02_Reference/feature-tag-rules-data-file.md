# Feature Tag Rules Data File

The shipped feature-tag query set keeps two paths:

- `Forward Device Feature Tags`: default map that requires no Forward data file and tags devices with `Prot_BGP` when Forward exposes structured BGP protocol state.
- `Forward Device Feature Tags with Rules`: disabled optional map that uses a Forward data file to control which structured features become NetBox device tags.

The default map remains the safe default. Enable the rules-aware variant only after the selected Forward snapshot exposes the required data file.

The rules-aware query text is shaped device-first (`foreach device in network.devices`) so Forward can use its automatic per-device NQE execution path on large networks. Keep that shape if you copy and customize the query in the Forward Org Repository.

## Required Forward Data File

Create a Forward data file with:

- File name: `netbox_feature_tag_rules.json`
- NQE name: `netbox_feature_tag_rules`
- File type: `JSON`

Attach the data file to the Forward network used by the sync, then run or reprocess a Forward snapshot. The plugin executes public `/api/nqe` against the selected snapshot, so the selected sync snapshot must include the uploaded data file value before rules affect plugin results.

If this data file is not uploaded, attached, and visible in the selected snapshot, do not enable `Forward Device Feature Tags with Rules`. Forward's NQE schema only exposes data files that exist for the network, so a query that references a missing data file will fail validation. Keep `Forward Device Feature Tags` enabled instead.

## Build The JSON

Generate the default structured BGP rule template:

```bash
python scripts/build_netbox_feature_tag_rules.py \
  --output netbox_feature_tag_rules.json
```

The generated JSON is a flat array with a stable schema:

```json
[
  {
    "record_type": "structured_feature_tag_rule",
    "feature": "bgp",
    "tag": "Prot_BGP",
    "tag_slug": "prot-bgp",
    "tag_color": "2196f3",
    "enabled": true,
    "match_source": "forward_structured_protocol",
    "description": "Tag devices where Forward exposes structured BGP protocol state."
  }
]
```

The initial optional query supports `record_type: structured_feature_tag_rule` with `feature: bgp`. Multiple enabled rows for the same feature are allowed, so a BGP-enabled device can receive more than one tag if that is intentional.

## Enable In NetBox

1. Upload and attach `netbox_feature_tag_rules.json` in Forward.
2. Run or reprocess a Forward snapshot after the upload.
3. Confirm the selected snapshot exposes `network.extensions.netbox_feature_tag_rules.value`.
4. In NetBox, open `Plugins > Forward Networks > NQE Maps`.
5. Disable `Forward Device Feature Tags`.
6. Enable `Forward Device Feature Tags with Rules`.
7. For large datasets, copy the rules-aware query text into the Forward Org Repository and configure the NetBox NQE map to use the committed query ID.

## Future Rule Types

Keep structured features as the first choice because they are parsed by Forward and are less vendor-specific than raw configuration text. Raw configuration matching can be added later as a separate `record_type`, for example `config_text_tag_rule`, without changing the data file name or the NetBox sync adapter.
