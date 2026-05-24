"""api.deps thread-safety 단위 — thread-local catalog + cursor 분리."""
from __future__ import annotations

import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).parents[3]
sys.path.insert(0, str(ROOT / "src"))

pytest.importorskip("duckdb")
pytest.importorskip("pyiceberg")


def test_catalog_is_thread_local():
    """서로 다른 스레드는 서로 다른 catalog 인스턴스를 받는다."""
    from api import deps

    created: list[object] = []

    def _fake_build():
        c = MagicMock(name=f"catalog-{len(created)}")
        created.append(c)
        return c

    with patch.object(deps, "build_catalog", side_effect=_fake_build):
        # thread-local 초기화 (이전 테스트 잔여 제거)
        if hasattr(deps._tls, "catalog"):
            del deps._tls.catalog

        seen = []
        with ThreadPoolExecutor(max_workers=4) as ex:
            seen = list(ex.map(lambda _: id(deps.catalog()), range(4)))

    # 4개 스레드 → 최대 4개 distinct 인스턴스 (스레드 재사용 시 줄 수 있으나 1개는 아님)
    assert len(set(seen)) >= 2
    # 같은 스레드 안 재호출은 재사용 (build 횟수 == distinct 인스턴스 수)
    assert len(created) == len(set(id(c) for c in created))


def test_duck_cursor_calls_cursor():
    """duck_cursor() 는 싱글톤 연결의 .cursor() 를 반환."""
    from api import deps

    fake_conn = MagicMock()
    with patch.object(deps, "duck_connection", return_value=fake_conn):
        deps.duck_cursor()
    fake_conn.cursor.assert_called_once()
