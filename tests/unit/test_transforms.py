from flink_jobs.lib.region_lookup import lookup
from flink_jobs.lib.transforms import (
    enrich_hotspot_silver,
    normalize_congest_level,
    sanitize_population,
)


def test_lookup_known_area():
    r = lookup("POI001")
    assert r is not None
    assert r.district == "강남구"
    assert r.gu_code == "11680"


def test_lookup_unknown_area_returns_none():
    assert lookup("POI999") is None


def test_normalize_congest_level_maps_korean_to_score():
    assert normalize_congest_level("여유") == 1
    assert normalize_congest_level("보통") == 2
    assert normalize_congest_level("약간 붐빔") == 3
    assert normalize_congest_level("붐빔") == 4
    assert normalize_congest_level("알 수 없음") == 0
    assert normalize_congest_level(" 여유 ") == 1  # strip 적용 검증


def test_sanitize_population_swaps_min_max_when_inverted():
    assert sanitize_population(40000, 30000) == (30000, 40000)
    assert sanitize_population(30000, 40000) == (30000, 40000)
    assert sanitize_population(None, 40000) == (None, 40000)


def test_enrich_hotspot_silver_adds_district_and_score():
    bronze = {
        "area_code": "POI001",
        "area_name": "강남역",
        "congest_level": "붐빔",
        "population_min": 42000,
        "population_max": 44000,
        "api_response_ts": "2026-04-30T14:25:00",
    }
    silver = enrich_hotspot_silver(bronze)
    assert silver["district"] == "강남구"
    assert silver["gu_code"] == "11680"
    assert silver["congest_level_score"] == 4
    assert silver["population_min"] == 42000
    assert silver["population_max"] == 44000
    assert "silver_arrival_ts" in silver


def test_enrich_hotspot_silver_drops_unknown_area():
    bronze = {
        "area_code": "POI999",
        "area_name": "Unknown",
        "congest_level": "보통",
        "population_min": 0,
        "population_max": 0,
        "api_response_ts": "2026-04-30T14:25:00",
    }
    assert enrich_hotspot_silver(bronze) is None
