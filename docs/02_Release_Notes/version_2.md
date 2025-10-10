---
description: v2 Release Notes
---

# v2 Release Notes

## v2.0.6 (2023-05-14)

### Enhancements
- `SD-629` - Add user agent information for Forward logs.

## v2.0.5 (2023-04-30)

### Enhancements
- `SD-638` - Add sync tenant information and or use it in transform maps.

## v2.0.4 (2023-03-18)

### Enhancements
- `SD-618` - Add interface speed to interface transform map.

### Bug Fixes
- `SD-619` - Fix topology not using correct source.

## v2.0.3 (2023-03-14)

### Enhancements
- `SD-617` - Allow timeout to be defined per source.

### Bug Fixes
- `SD-617` - Tables with a lot of data cause timeout issues handle gracefully.

## v2.0.2 (2023-03-11)

### Bug Fixes
- `SD-615` - Fix issue with snapshot data saving.

## v2.0.1 (2023-03-08)

### Enhancements
- `SD-583`- Fetch Forward topology via site model.

### Bug Fixes
- `SD-614` - Fix issue snapshot model serialization.

## v2.0.0 (2023-02-04)

### Enhancements

- `SD-564` - Support NetBox 3.7.
- `SD-566` - Add ability to restore transform maps to their default.
- `SD-570` - Move Forward to a top level navigation item.
- `SD-572` - Install transform maps by default if none are pre-installed.

### Bug Fixes

- `SD-567` - Fix issue with cache not working on Device Forward Table.
- `SD-568` - Fix issue with snapshot api.
- `SD-571` - Fix issue with ingestion sync button not being greyed out if no raw data exists.
- `SD-574` - Remove unused code.
