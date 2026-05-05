import json
from pathlib import Path

import pytest

FIXTURE_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def hotspot_sample() -> dict:
    return json.loads((FIXTURE_DIR / "seoul_hotspot_sample.json").read_text(encoding="utf-8"))


@pytest.fixture
def hotspot_real_sample() -> dict:
    return json.loads((FIXTURE_DIR / "seoul_hotspot_real_sample.json").read_text(encoding="utf-8"))


@pytest.fixture
def hotspot_list_live_only() -> dict:
    """LIVE_PPLTN_STTS 만 list, 나머지 둘은 dict."""
    return json.loads((FIXTURE_DIR / "seoul_hotspot_list_live_only.json").read_text(encoding="utf-8"))


@pytest.fixture
def hotspot_list_road_only() -> dict:
    """ROAD_TRAFFIC_STTS 만 list, 나머지 둘은 dict."""
    return json.loads((FIXTURE_DIR / "seoul_hotspot_list_road_only.json").read_text(encoding="utf-8"))


@pytest.fixture
def hotspot_list_weather_only() -> dict:
    """WEATHER_STTS 만 list, 나머지 둘은 dict."""
    return json.loads((FIXTURE_DIR / "seoul_hotspot_list_weather_only.json").read_text(encoding="utf-8"))


@pytest.fixture
def subway_sample() -> dict:
    return json.loads((FIXTURE_DIR / "seoul_subway_sample.json").read_text(encoding="utf-8"))
