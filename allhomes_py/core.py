"""Core implementation of the allhomes_py package.

Exposes `get_past_sales_data` for fetching historical property sales via
the Allhomes GraphQL API.
"""
import json
import re
import time
import warnings
from pathlib import Path
from typing import Callable, Optional

import polars as pl
import requests

_DATA_DIR = Path(__file__).resolve().parent / "_data"


def _load_postcode_map(csv_path: Path) -> dict[str, str]:
    """Return a lowercase-division → postcode mapping built from the given CSV."""
    rows = pl.read_csv(csv_path)
    return {
        name.lower(): str(code)
        for name, code in zip(
            rows["division"].to_list(), rows["postcode"].to_list()
        )
    }


_DIVISIONS_ACT = _load_postcode_map(_DATA_DIR / "divisions_ACT.csv")
_DIVISIONS_NSW = _load_postcode_map(_DATA_DIR / "divisions_NSW.csv")
_MAX_ENTRIES = 5000
_GRAPHQL_URL = "https://www.allhomes.com.au/graphql"
_PERSISTED_HASH = (
    "d16064a1e14de8b8192be6bece8e2bb0dec81e1d46d0736461fd8c9484211996"
)
_TRANSIENT_STATUS = {429, 500, 502, 503, 504}
_MAX_RETRY_ATTEMPTS = 5
_REQUEST_TIMEOUT_SECONDS = 20


def get_divisions_data(state: str) -> pl.DataFrame:
    """Return suburb divisions data for the requested state (ACT or NSW)."""
    normalized_state = state.upper() if isinstance(state, str) else None
    if normalized_state not in {"ACT", "NSW"}:
        raise ValueError("`state` must be either 'ACT' or 'NSW'.")

    csv_file = _DATA_DIR / f"divisions_{normalized_state}.csv"
    return pl.read_csv(csv_file)


def get_past_sales_data(suburb: str, year: Optional[int] = None, max_entries: int = 5000) -> pl.DataFrame:
    """Retrieve past sales data for an ACT/NSW suburb from Allhomes."""
    if year is not None and (not isinstance(year, int) or isinstance(year, bool) or year <= 0):
        raise ValueError("`year` must be None or a positive integer.")
    if not 1 <= max_entries <= _MAX_ENTRIES:
        raise ValueError("`max_entries` must be a value between 1 and 5000.")

    slug = _format_slug(suburb)
    json_data = _fetch_sales_history_json(page=1, page_size=max_entries, slug=slug, year=year)
    df = _format_sales_data_from_json(json_data)

    if df.height == max_entries:
        warnings.warn("Truncated data. Increase `max_entries` or rerun query in year batches!", UserWarning)
    elif df.is_empty():
        warnings.warn(f"No sales data for suburb '{suburb}'", UserWarning)

    return df


def _validate_suburb(suburb: str) -> tuple[str, str, Optional[str]]:
    """Parse and validate a suburb string; return (division, state, postcode)."""
    parts = [segment.strip() for segment in suburb.split(",")]
    if len(parts) != 2:
        raise ValueError("`suburb` must contain name and state abbreviation separated by a comma")

    division, state = parts[0], parts[1].lower()
    if state not in {"act", "nsw"}:
        raise ValueError("Currently only data for ACT and NSW suburbs are available")

    lookup = _DIVISIONS_ACT if state == "act" else _DIVISIONS_NSW
    postcode = lookup.get(division.lower())
    if not postcode:
        raise ValueError(
            f"Could not validate suburb '{suburb}'. "
            "Could not find a matching postcode."
        )

    return division, state, postcode


def _format_slug(suburb: str) -> str:
    """Return the Allhomes URL slug for a suburb string (e.g. ``"abbotsford-nsw-2046"``)."""
    division, state, postcode = _validate_suburb(suburb)
    slug = f"{division}-{state}-{postcode}".lower()
    slug = re.sub(r"\s+", "-", slug)
    return slug


def _construct_sales_history_request(page: int, page_size: int, slug: str, year: Optional[int]) -> tuple[str, dict, dict]:
    """Build the URL, query params, and headers for a sales history GraphQL request."""
    duration = {"unit": "ALL"} if year is None else {"unit": "SPECIFIC_YEAR", "duration": year}

    variables = {
        "locality": {"slug": slug, "type": "DIVISION"},
        "filters": {"beds": {"lower": 0}, "baths": {"lower": 0}, "parks": {"lower": 0}},
        "duration": duration,
        "sort": {"type": "SOLD_AGE", "order": "DESC"},
        "page": page,
        "pageSize": page_size,
    }

    extensions = {
        "persistedQuery": {
            "version": 1,
            "sha256Hash": _PERSISTED_HASH,
        }
    }

    params = {
        "operationName": "updateHistoryForLocality",
        "variables": json.dumps(variables, separators=(",", ":")),
        "extensions": json.dumps(extensions, separators=(",", ":")),
    }

    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
        "x-apollo-operation-name": "updateHistoryForLocality",
    }

    return _GRAPHQL_URL, params, headers


def _fetch_sales_history_json(
    page: int,
    page_size: int,
    slug: str,
    year: Optional[int],
    request_get: Callable = requests.get,
    sleep_func: Callable = time.sleep,
    max_retry_attempts: int = _MAX_RETRY_ATTEMPTS,
    timeout_seconds: int = _REQUEST_TIMEOUT_SECONDS,
) -> dict:
    """Fetch the sales history JSON payload, retrying on transient HTTP errors."""
    url, params, headers = _construct_sales_history_request(page, page_size, slug, year)
    last_error = None

    for attempt in range(1, max_retry_attempts + 1):
        try:
            response = request_get(url, params=params, headers=headers, timeout=timeout_seconds)
            if response.status_code in _TRANSIENT_STATUS:
                raise requests.HTTPError(f"Transient status {response.status_code}")
            response.raise_for_status()
            payload = response.json()
            graphql_errors = payload.get("errors") if isinstance(payload, dict) else None
            if graphql_errors:
                raise RuntimeError(f"GraphQL errors returned: {graphql_errors}")
            return payload
        except requests.RequestException as exc:
            last_error = exc
            if attempt == max_retry_attempts:
                raise
            wait = 2 ** (attempt - 1)
            sleep_func(wait)

    raise RuntimeError("Failed to fetch sales history JSON") from last_error


def _flatten_node(node: dict) -> dict:
    """Flatten a single GraphQL sales node into a flat result dict with named columns."""
    flattened = {}

    def recurse(value, prefix=""):
        if isinstance(value, dict):
            for key, child in value.items():
                recurse(child, f"{prefix}{key}_")
        else:
            flattened[prefix[:-1]] = value

    if not isinstance(node, dict):
        return flattened

    for field in ["listing", "features", "address", "address_division", "address_state", "transfer"]:
        recurse(node.get(field, {}) or {}, f"{field}_")

    agents = node.get("agents") or []
    for index in range(2):
        agent = agents[index] if index < len(agents) and isinstance(agents[index], dict) else {}
        recurse(agent, f"agents_{index + 1}_")
        recurse(agent.get("agency", {}) or {}, f"agents_{index + 1}_agency_")

    for key, value in node.items():
        if key != "agents":
            flattened.setdefault(key, value)

    result = {
        "contract_date": flattened.get("transfer_contractDate"),
        "address": flattened.get("address_line1"),
        "division": flattened.get("address_division_name"),
        "state": flattened.get("address_state_abbreviation"),
        "postcode": flattened.get("address_postcode"),
        "url": flattened.get("listing_url"),
        "image_url": flattened.get("listing_firstImage_imageSrc") or None,
        "property_type": flattened.get("features_propertyType"),
        "purpose": flattened.get("transfer_purpose"),
        "bedrooms": flattened.get("features_bedrooms"),
        "bathrooms": flattened.get("features_bathrooms_total"),
        "parking": flattened.get("features_parking_total"),
        "building_size": flattened.get("features_buildingSize"),
        "block_size": flattened.get("transfer_blockSize"),
        "eer": flattened.get("features_eer"),
        "list_date": flattened.get("listing_publicVisibleDate"),
        "transfer_date": flattened.get("transfer_transferDate"),
        "days_on_market": flattened.get("listing_daysOnMarket"),
        "label": flattened.get("transfer_label"),
        "price": flattened.get("transfer_price"),
        "agent": flattened.get("agents_1_agency_name"),
        "unimproved_value": flattened.get("transfer_unimprovedValue"),
        "unimproved_value_ratio": flattened.get("transfer_unimprovedValueRatio"),
    }

    return result


def _format_sales_data_from_json(json_data: dict) -> pl.DataFrame:
    """Convert a raw GraphQL JSON response into a typed Polars DataFrame."""
    nodes = (
        json_data
        .get("data", {})
        .get("historyForLocality", {})
        .get("nodes", [])
    )

    if not nodes:
        return pl.DataFrame([])

    rows = [_flatten_node(node) for node in nodes]
    # API payloads can mix types across rows (for example int/string/null),
    # so ingest non-strictly to avoid schema-append failures.
    df = pl.from_dicts(rows, strict=False, infer_schema_length=None)

    def parse_date(column_name: str):
        if column_name in df.columns:
            # Some API fields are timestamps like "2026-03-31T00:00:00".
            # Slice to the first 10 characters so both timestamps and plain dates parse.
            return (
                pl.col(column_name)
                .cast(pl.Utf8)
                .str.slice(0, 10)
                .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
            )
        return None

    df = df.with_columns([
        parse_date("contract_date"),
        parse_date("list_date"),
        parse_date("transfer_date"),
        pl.col("unimproved_value").cast(pl.Int64, strict=False),
        pl.col("unimproved_value_ratio").cast(pl.Float64, strict=False),
        pl.col("price").cast(pl.Int64, strict=False),
    ])

    # Impute missing contract_date as list_date + days_on_market when both are available.
    df = df.with_columns(
        pl.when(
            pl.col("contract_date").is_null()
            & pl.col("list_date").is_not_null()
            & pl.col("days_on_market").is_not_null()
        )
        .then(pl.col("list_date") + pl.duration(days=pl.col("days_on_market").cast(pl.Int32, strict=False)))
        .otherwise(pl.col("contract_date"))
        .alias("contract_date")
    )

    return df
