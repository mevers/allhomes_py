# Changelog

All notable changes to this project are documented in this file.

## v0.3.3 (2026-05-14)

- Fixed `sa3_name` and `sa4_name` enrichment to join on division, state, and postcode, preventing incorrect matches and duplicate rows for ambiguous suburb names such as `Mayfield, NSW`.
- Added regression coverage for ambiguous suburb SA3/SA4 joins.

## v0.3.2 (2026-05-12)

- Added support for optional postcode values in suburb strings, e.g. `Mayfield, NSW, 2304`.
- Fixed ambiguous suburb validation so duplicate division/state combinations warn and require a postcode instead of silently choosing one postcode.
- Added known data issues documentation in `docs/KNOWN_DATA_ISSUES.md`.

## v0.3.1 (2026-04-20)

- Added `sa3_name` and `sa4_name` to the output from `get_past_sales_data()`.

## v0.3.0 (2026-04-20)

- Updated packaged division data files in `allhomes_py/_data`.
