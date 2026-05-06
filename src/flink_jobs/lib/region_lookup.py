"""핫스팟 → 자치구 매핑. CSV 한 번 로드 후 dict 캐시."""
from __future__ import annotations

import csv
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

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
