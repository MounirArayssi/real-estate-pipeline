"""Tests for the scraper module."""

import pytest
from src.scraper import parse_listing


# Sample API response matching real structure
SAMPLE_RAW = {
    "property_id": "9269556571",
    "listing_id": "2991449955",
    "status": "for_sale",
    "photo_count": 39,
    "location": {
        "address": {
            "city": "Los Angeles",
            "line": "211 S Berendo St Apt 3",
            "postal_code": "90004",
            "state_code": "CA",
            "coordinate": {"lat": 34.070454, "lon": -118.294502},
        }
    },
    "description": {
        "type": "condos",
        "sub_type": "condo",
        "beds": 1,
        "baths": 1,
        "sqft": 712,
        "lot_sqft": 9387,
    },
    "flags": {
        "is_new_listing": True,
        "is_foreclosure": None,
        "is_price_reduced": None,
    },
    "list_price": 439999,
    "list_date": "2026-02-11T02:08:34.000000Z",
    "price_reduced_amount": None,
    "last_sold_price": None,
    "last_sold_date": None,
    "primary_photo": {"href": "https://example.com/photo.jpg"},
    "href": "https://www.realtor.com/detail/test",
}


def test_parse_listing_basic_fields():
    result = parse_listing(SAMPLE_RAW)
    assert result["property_id"] == "9269556571"
    assert result["city"] == "Los Angeles"
    assert result["state"] == "CA"
    assert result["zip"] == "90004"
    assert result["price"] == 439999
    assert result["beds"] == 1
    assert result["sqft"] == 712


def test_parse_listing_computed_fields():
    result = parse_listing(SAMPLE_RAW)
    assert result["lat"] == 34.070454
    assert result["lon"] == -118.294502
    assert result["property_type"] == "condos"
    assert result["property_subtype"] == "condo"


def test_parse_listing_date_parsing():
    result = parse_listing(SAMPLE_RAW)
    from datetime import date
    assert result["list_date"] == date(2026, 2, 11)


def test_parse_listing_handles_missing_data():
    minimal = {"property_id": "123", "location": {}, "description": {}}
    result = parse_listing(minimal)
    assert result["property_id"] == "123"
    assert result["city"] is None
    assert result["price"] is None
    assert result["beds"] is None
    assert result["list_date"] is None


def test_parse_listing_handles_null_photo():
    no_photo = {**SAMPLE_RAW, "primary_photo": None}
    result = parse_listing(no_photo)
    assert result["photo_url"] is None