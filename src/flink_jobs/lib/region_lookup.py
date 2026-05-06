"""핫스팟 → 자치구 매핑. CSV 한 번 로드 후 dict 캐시."""
from __future__ import annotations

import csv
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

# 본 파일 위치 = src/flink_jobs/lib/region_lookup.py → parents[3] = repo root.
# lib 하위 추가 중첩 / 파일 이동 시 parents 인덱스 재검토 필요.
REFERENCE_PATH = Path(__file__).resolve().parents[3] / "data" / "reference" / "hotspot_regions.csv"


@dataclass(frozen=True)
class Region:
    area_code: str
    area_name: str
    district: str
    gu_code: str
    latitude: float
    longitude: float


@lru_cache
def _load() -> dict[str, Region]:
    if not REFERENCE_PATH.exists():
        raise FileNotFoundError(f"hotspot_regions.csv not found at {REFERENCE_PATH}")
    out: dict[str, Region] = {}
    with REFERENCE_PATH.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            out[row["area_code"]] = Region(
                area_code=row["area_code"],
                area_name=row["area_name"],
                district=row["district"],
                gu_code=row["gu_code"],
                latitude=float(row["latitude"]),
                longitude=float(row["longitude"]),
            )
    return out


def lookup(area_code: str) -> Region | None:
    return _load().get(area_code)


def all_regions() -> dict[str, Region]:
    return _load()
