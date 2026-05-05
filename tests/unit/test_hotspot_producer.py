from datetime import UTC, datetime

import pytest

from producers.hotspot_producer import parse_hotspot_payload
from producers.schemas import HotspotEvent


def test_parse_hotspot_payload_extracts_core_fields(hotspot_sample):
    event = parse_hotspot_payload(hotspot_sample, area_code="POI001")

    assert isinstance(event, HotspotEvent)
    assert event.area_code == "POI001"
    assert event.area_name == "강남역"
    assert event.congest_level == "붐빔"
    assert event.population_min == 42000
    assert event.population_max == 44000
    assert event.api_response_ts == datetime(2026, 4, 30, 14, 25, tzinfo=UTC).replace(tzinfo=None)
    # 공기/도로/날씨는 옵셔널
    assert event.road_traffic_index == "서행"
    assert event.temperature_c == 21.3


def test_parse_hotspot_payload_returns_none_when_missing(hotspot_sample):
    bad = {"RESULT": {"RESULT.CODE": "ERROR-500"}}
    assert parse_hotspot_payload(bad, area_code="POI001") is None


def test_hotspot_event_kafka_key_is_area_code(hotspot_sample):
    event = parse_hotspot_payload(hotspot_sample, area_code="POI001")
    assert event.kafka_key() == "POI001"


def test_hotspot_event_kafka_headers_includes_api_response_ts(hotspot_sample):
    event = parse_hotspot_payload(hotspot_sample, area_code="POI001")
    headers = dict(event.kafka_headers())
    assert "api_response_ts" in headers
    # 헤더는 bytes
    assert headers["api_response_ts"] == b"2026-04-30T14:25:00"
    assert headers["schema_version"] == b"v1"


def test_parse_with_list_live_ppltn_stts(hotspot_list_live_only):
    """LIVE_PPLTN_STTS 만 list 인 경우 혼잡도·인구 필드를 정상 파싱한다."""
    event = parse_hotspot_payload(hotspot_list_live_only, area_code="POI001")
    assert event is not None
    assert isinstance(event, HotspotEvent)
    assert event.congest_level == "붐빔"
    assert event.population_min == 42000


def test_parse_with_list_road_traffic_stts(hotspot_list_road_only):
    """ROAD_TRAFFIC_STTS 만 list 인 경우 도로 필드를 정상 파싱한다."""
    event = parse_hotspot_payload(hotspot_list_road_only, area_code="POI001")
    assert event is not None
    assert event.road_traffic_index == "서행"
    assert event.road_traffic_speed_kmh == pytest.approx(18.4)


def test_parse_with_list_weather_stts(hotspot_list_weather_only):
    """WEATHER_STTS 만 list 인 경우 기상 필드를 정상 파싱한다."""
    event = parse_hotspot_payload(hotspot_list_weather_only, area_code="POI001")
    assert event is not None
    assert event.temperature_c == pytest.approx(21.3)
    assert event.precipitation == "없음"


def test_parse_with_all_stts_as_list(hotspot_real_sample):
    """실 API 형태(세 *_STTS 모두 list)에서 전체 파싱이 정상 동작한다 — smoke test."""
    event = parse_hotspot_payload(hotspot_real_sample, area_code="POI001")
    assert event is not None
    assert event.congest_level == "붐빔"
    assert event.road_traffic_index == "서행"
    assert event.temperature_c == pytest.approx(21.3)
