"""Tests for allhomes_py.core: JSON parsing, input validation, retry behaviour, and user warnings."""
import datetime

import pytest
import polars as pl
import requests

import allhomes_py.core as core
from allhomes_py.core import (
    _fetch_sales_history_json,
    _format_sales_data_from_json,
    get_divisions_data,
    get_past_sales_data,
)


class DummyResponse:
    """Minimal stand-in for a ``requests.Response`` used in HTTP fetch tests."""

    def __init__(self, status_code=200, payload=None, http_error=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self._http_error = http_error

    def raise_for_status(self):
        if self._http_error is not None:
            raise self._http_error

    def json(self):
        return self._payload


def test_image_url_mapping_from_listing_first_image():
    json_data = {
        "data": {
            "historyForLocality": {
                "nodes": [
                    {
                        "listing": {
                            "firstImage": {
                                "imageSrc": "https://images.allhomes.com.au/sample.jpg"
                            },
                            "listing_url": "https://www.allhomes.com.au/property/sample",
                        }
                    }
                ]
            }
        }
    }

    df = _format_sales_data_from_json(json_data)

    assert isinstance(df, pl.DataFrame)
    assert df.height == 1
    assert df["image_url"].to_list() == ["https://images.allhomes.com.au/sample.jpg"]


def test_image_url_is_null_when_missing():
    json_data = {
        "data": {
            "historyForLocality": {
                "nodes": [
                    {
                        "listing": {},
                    }
                ]
            }
        }
    }

    df = _format_sales_data_from_json(json_data)

    assert df["image_url"].to_list() == [None]


def test_mixed_schema_rows_are_handled_non_strictly():
    json_data = {
        "data": {
            "historyForLocality": {
                "nodes": [
                    {
                        "features": {"parking": {"total": "N/A"}},
                        "transfer": {"price": "not disclosed"},
                    },
                    {
                        "features": {"parking": {"total": 0}},
                        "transfer": {"price": 750000},
                    },
                ]
            }
        }
    }

    df = _format_sales_data_from_json(json_data)

    assert isinstance(df, pl.DataFrame)
    assert df.height == 2
    assert df["price"].to_list() == [None, 750000]


def test_contract_date_imputed_from_list_date_and_days_on_market():
    json_data = {
        "data": {
            "historyForLocality": {
                "nodes": [
                    {
                        # contract_date absent: should be imputed as list_date + days_on_market
                        "listing": {"publicVisibleDate": "2023-01-01", "daysOnMarket": 30},
                    },
                    {
                        # contract_date present: must not be overwritten
                        "listing": {"publicVisibleDate": "2023-01-01", "daysOnMarket": 30},
                        "transfer": {"contractDate": "2023-03-15"},
                    },
                    {
                        # list_date absent: contract_date must remain null
                        "listing": {"daysOnMarket": 30},
                    },
                    {
                        # days_on_market absent: contract_date must remain null
                        "listing": {"publicVisibleDate": "2023-01-01"},
                    },
                ]
            }
        }
    }

    df = _format_sales_data_from_json(json_data)
    dates = df["contract_date"].to_list()

    assert dates[0] == datetime.date(2023, 1, 31)  # 2023-01-01 + 30 days
    assert dates[1] == datetime.date(2023, 3, 15)  # unchanged
    assert dates[2] is None                         # list_date is null
    assert dates[3] is None                         # days_on_market is null


@pytest.mark.parametrize("state", ["ACT", "act", "NSW", "nsw"])
def test_get_divisions_data_returns_dataframe_for_valid_state(state):
    df = get_divisions_data(state)

    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert {"division", "postcode"}.issubset(set(df.columns))


@pytest.mark.parametrize("state", ["VIC", "", None])
def test_get_divisions_data_rejects_invalid_state(state):
    with pytest.raises(ValueError, match="`state` must be either 'ACT' or 'NSW'."):
        get_divisions_data(state)


@pytest.mark.parametrize("year", ["2023", 2023.0, -2023, [2021, 2022]])
def test_get_past_sales_data_rejects_invalid_year_type(year):
    with pytest.raises(ValueError, match="`year` must be None or a positive integer"):
        get_past_sales_data("Abbotsford, NSW", year=year)


@pytest.mark.parametrize("max_entries", [0, 5001])
def test_get_past_sales_data_rejects_invalid_max_entries(max_entries):
    with pytest.raises(ValueError, match="`max_entries` must be a value between 1 and 5000"):
        get_past_sales_data("Abbotsford, NSW", max_entries=max_entries)


def test_get_past_sales_data_rejects_invalid_suburb_format():
    with pytest.raises(ValueError, match="must contain name and state abbreviation"):
        get_past_sales_data("Abbotsford")


def test_get_past_sales_data_rejects_unsupported_state():
    with pytest.raises(ValueError, match="only data for ACT and NSW"):
        get_past_sales_data("Abbotsford, VIC")


def test_get_past_sales_data_rejects_unknown_suburb():
    with pytest.raises(ValueError, match="Could not validate suburb"):
        get_past_sales_data("NotARealSuburb, NSW")


def test_fetch_retries_transient_status_then_succeeds():
    responses = [
        DummyResponse(status_code=429, payload={}),
        DummyResponse(status_code=200, payload={"data": {"historyForLocality": {"nodes": []}}}),
    ]
    sleep_calls = []

    def fake_get(*_args, **_kwargs):
        return responses.pop(0)

    payload = _fetch_sales_history_json(
        page=1,
        page_size=10,
        slug="abbotsford-nsw-2046",
        year=None,
        request_get=fake_get,
        sleep_func=sleep_calls.append,
    )

    assert payload == {"data": {"historyForLocality": {"nodes": []}}}
    assert sleep_calls == [1]


def test_fetch_raises_after_exhausting_transient_retries():
    sleep_calls = []

    def always_transient(*_args, **_kwargs):
        return DummyResponse(status_code=503, payload={})

    with pytest.raises(requests.HTTPError, match="Transient status 503"):
        _fetch_sales_history_json(
            page=1,
            page_size=10,
            slug="abbotsford-nsw-2046",
            year=None,
            request_get=always_transient,
            sleep_func=sleep_calls.append,
        )

    assert len(sleep_calls) == 4


def test_fetch_retries_request_exception_then_succeeds():
    calls = {"count": 0}
    sleep_calls = []

    def flaky_get(*_args, **_kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            raise requests.Timeout("timeout")
        return DummyResponse(status_code=200, payload={"data": {"historyForLocality": {"nodes": []}}})

    payload = _fetch_sales_history_json(
        page=1,
        page_size=10,
        slug="abbotsford-nsw-2046",
        year=None,
        request_get=flaky_get,
        sleep_func=sleep_calls.append,
    )

    assert payload == {"data": {"historyForLocality": {"nodes": []}}}
    assert sleep_calls == [1]


def test_fetch_raises_when_graphql_errors_present():
    def fake_get(*_args, **_kwargs):
        return DummyResponse(status_code=200, payload={"errors": [{"message": "boom"}]})

    with pytest.raises(RuntimeError, match="GraphQL errors returned"):
        _fetch_sales_history_json(
            page=1,
            page_size=10,
            slug="abbotsford-nsw-2046",
            year=None,
            request_get=fake_get,
            sleep_func=lambda *_: None,
        )


def test_get_past_sales_data_warns_on_empty_result(monkeypatch):
    monkeypatch.setattr(core, "_format_slug", lambda _suburb: "abbotsford-nsw-2046")
    monkeypatch.setattr(
        core,
        "_fetch_sales_history_json",
        lambda **_kwargs: {"data": {"historyForLocality": {"nodes": []}}},
    )

    with pytest.warns(UserWarning, match="No sales data"):
        df = get_past_sales_data("Abbotsford, NSW", max_entries=3)

    assert df.is_empty()


def test_get_past_sales_data_warns_when_data_truncated(monkeypatch):
    monkeypatch.setattr(core, "_format_slug", lambda _suburb: "abbotsford-nsw-2046")
    monkeypatch.setattr(
        core,
        "_fetch_sales_history_json",
        lambda **_kwargs: {
            "data": {
                "historyForLocality": {
                    "nodes": [
                        {"transfer": {"price": 100}},
                        {"transfer": {"price": 200}},
                        {"transfer": {"price": 300}},
                    ]
                }
            }
        },
    )

    with pytest.warns(UserWarning, match="Truncated data"):
        df = get_past_sales_data("Abbotsford, NSW", max_entries=3)

    assert df.height == 3
