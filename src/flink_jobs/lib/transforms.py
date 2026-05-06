"""Bronze JSON dict → Silver dict 의 순수 변환.
PyFlink 환경 없이 단위 테스트 가능."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from .region_lookup import lookup

CONGEST_LEVEL_MAP = {
    "여유": 1,
    "보통": 2,
    "약간 붐빔": 3,
    "붐빔": 4,
}


def normalize_congest_level(level: str | None) -> int:
    if not level:
        return 0
    return CONGEST_LEVEL_MAP.get(level.strip(), 0)


def sanitize_population(p_min: int | None, p_max: int | None) -> tuple[int | None, int | None]:
    if p_min is None or p_max is None:
        return p_min, p_max
    if p_min > p_max:
        return p_max, p_min
    return p_min, p_max


def enrich_hotspot_silver(bronze: dict[str, Any]) -> dict[str, Any] | None:
    """Bronze dict 를 silver dict 로 변환. region 미매핑 시 None drop.

    silver_arrival_ts 는 본 함수 진입 시점의 UTC wall clock 을 naive isoformat 으로 박는다.
    Iceberg 의 timestamp without timezone 컬럼 매핑 가정.
    """
    region = lookup(bronze.get("area_code", ""))
    if region is None:
        return None

    p_min, p_max = sanitize_population(
        bronze.get("population_min"),
        bronze.get("population_max"),
    )

    return {
        "area_code": region.area_code,
        "area_name": region.area_name,
        "district": region.district,
        "gu_code": region.gu_code,
        "latitude": region.latitude,
        "longitude": region.longitude,
        "congest_level": bronze.get("congest_level"),
        "congest_level_score": normalize_congest_level(bronze.get("congest_level")),
        "congest_message": bronze.get("congest_message"),
        "population_min": p_min,
        "population_max": p_max,
        "road_traffic_index": bronze.get("road_traffic_index"),
        "road_traffic_speed_kmh": bronze.get("road_traffic_speed_kmh"),
        "temperature_c": bronze.get("temperature_c"),
        "precipitation": bronze.get("precipitation"),
        "api_response_ts": bronze.get("api_response_ts"),
        "silver_arrival_ts": datetime.now(UTC).replace(tzinfo=None).isoformat(),
    }
