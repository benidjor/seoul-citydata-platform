"""iceberg_maintenance — Day 9 본격 활성 (rewrite_data_files + Discord 알림).

Day 5 Task 5.8 buffer 의 echo placeholder 7개 → 본격 활성 (6개로 reduction):

- snapshot_metrics_before/after = PythonOperator + pyiceberg lookup + XCom push
- rewrite_silver_hotspot_congestion = BashOperator + docker run scp/spark +
  spark-submit compaction_silver.py
- post_compaction_report = PythonOperator + XCom pull + send_compaction_report
  (Discord webhook + stdout fallback)
- expire_snapshots + remove_orphan_files = echo placeholder 유지 (Day 10 또는
  Phase 2 본격)

본진 기능 (spec §5-8 표 2행):

- 병렬 실행 — TaskGroup `rewrite` (현재 single child, P1B 후 rewrite_user_event
  추가 시 병렬 발휘).
- `max_active_tis_per_dag=3` — Spark concurrent submit 제한.
- XCom — before/after 메트릭 (files / bytes / snapshots).
- on_failure_callback — `send_discord_alert` (Discord webhook + stdout
  fallback).
- SLA 1시간 — 메모리 ceiling 위협 자동 감지.
- schedule `0 3 * * *` — 매일 03:00 KST (streaming peak 회피).

PR α (#53) + PR β (#54) deviation reuse (변경 0건):

- 9.1-A: warehouse=seoul (spark-defaults.conf).
- 9.1-B: iceberg-aws-bundle (Dockerfile).
- 9.1-C: extraClassPath 절대 경로 (spark-defaults.conf).
- 9.1-D: `docker run --rm` 자동 cleanup (정공 명령 정신 보존).
- 9.2-A 확장: procedure call argument 2-part `silver.hotspot_congestion`.

사용자 결정 사항 (Day 9 PR γ):

- A1: `rewrite_dim_place` task 제거 (P1B 후 활성화 의무, Plan SoT line 2316).
- A2: task_id rename `rewrite_fact_hotspot_congestion_5min` →
  `rewrite_silver_hotspot_congestion` (PR β 의 compaction_silver.py =
  silver.hotspot_congestion 대상과 정합).
- A3: `expire_snapshots` + `remove_orphan_files` placeholder 유지 (Day 10
  또는 Phase 2 본격).
- A4: docker socket mount + `docker run --rm` (Airflow image rebuild 회피).

보안 limitation:

- docker socket mount = Airflow 컨테이너가 host docker daemon 의 root 권한
  직접 사용. Phase 1A 데모 한정 (single-user laptop, public 공개 없음).
- Phase 2 Oracle Cloud 배포 시 Spark on Kubernetes / SparkSubmitOperator +
  Livy 또는 SSHOperator 로 재설계 의무.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from typing import Any

# PythonOperator in-process 호출 위해 PYTHONPATH 추가.
# dbt_full_run.py 의 BashOperator subprocess 패턴 (PYTHONPATH=/opt/airflow/repo-src)
# 의 PythonOperator equivalent — PythonOperator 는 in-process 라 sys.path 직접 주입.
# `_capture_metrics` 안에서 `flink_jobs.lib.duckdb_iceberg` import 시 필요.
sys.path.insert(0, "/opt/airflow/repo-src")  # noqa: E402

from airflow import DAG  # noqa: E402, I001
from airflow.operators.bash import BashOperator  # noqa: E402
from airflow.operators.python import PythonOperator  # noqa: E402
from airflow.utils.task_group import TaskGroup  # noqa: E402

from common.callbacks import send_compaction_report, send_discord_alert  # noqa: E402

log = logging.getLogger(__name__)

SILVER_TABLE = "silver.hotspot_congestion"
SPARK_IMAGE = "scp/spark:3.5.3-iceberg"
SPARK_NETWORK = "scp_default"  # compose project name `scp` SoT (docker-compose.yml L1)


def _capture_metrics(table: str, **context: Any) -> dict[str, Any]:
    """pyiceberg 로 Iceberg table 의 file / byte / snapshot 메트릭 측정 + XCom push.

    Day 9 PR γ — Airflow PythonOperator in-process 호출. dbt_full_run.py 의
    PYTHONPATH=/opt/airflow/repo-src env (BashOperator subprocess) 와 동일
    정신이지만, PythonOperator in-process 라 module-level `sys.path.insert`
    로 호출 의무.
    """
    from flink_jobs.lib.duckdb_iceberg import build_catalog

    catalog = build_catalog()
    iceberg_table = catalog.load_table(table)
    files = list(iceberg_table.scan().plan_files())
    n_files = len(files)
    total_bytes = sum(f.file.file_size_in_bytes for f in files)
    n_snapshots = len(list(iceberg_table.snapshots()))

    metrics = {
        "table": table,
        "files": n_files,
        "bytes": total_bytes,
        "snapshots": n_snapshots,
    }
    log.info("Metrics captured: %s", metrics)
    return metrics


default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "sla": timedelta(hours=1),
    "on_failure_callback": send_discord_alert,
}


with DAG(
    dag_id="iceberg_maintenance",
    description="Iceberg compaction + Discord 알림 (Day 9 Task 9.3 본격 활성)",
    start_date=datetime(2026, 5, 1),
    schedule="0 3 * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["airflow", "day9", "task9.3", "iceberg-maintenance"],
) as dag:
    snapshot_metrics_before = PythonOperator(
        task_id="snapshot_metrics_before",
        python_callable=_capture_metrics,
        op_kwargs={"table": SILVER_TABLE},
    )

    with TaskGroup("rewrite") as rewrite:
        # docker run --rm (compose plugin 부재 우회). --rm 자동 cleanup.
        # PR α (#53) 의 9.1-D 정공 명령 정신 보존.
        # PROJECT_ROOT = docker-compose.yml 의 airflow-common environment 에서
        # ${PWD} default 로 host CWD = 프로젝트 루트 주입 (Step 1).
        rewrite_silver_hotspot_congestion = BashOperator(
            task_id="rewrite_silver_hotspot_congestion",
            bash_command=(
                "set -e\n"
                "docker run --rm "
                f"--network {SPARK_NETWORK} "
                "-v ${PROJECT_ROOT}/infra/spark/conf:/opt/spark/conf:ro "
                "-v ${PROJECT_ROOT}/infra/spark/jobs:/workspace/jobs:ro "
                "-e AWS_ACCESS_KEY_ID=minioadmin "
                "-e AWS_SECRET_ACCESS_KEY=minioadmin "
                "-e AWS_REGION=us-east-1 "
                f"{SPARK_IMAGE} "
                "/opt/spark/bin/spark-submit /workspace/jobs/compaction_silver.py\n"
            ),
            max_active_tis_per_dag=3,
        )

    expire_snapshots = BashOperator(
        task_id="expire_snapshots",
        bash_command=(
            "echo '[Day 9 placeholder] expire_snapshots older_than 7d "
            "(Day 10 또는 Phase 2 본격 활성)'"
        ),
    )

    remove_orphan_files = BashOperator(
        task_id="remove_orphan_files",
        bash_command=(
            "echo '[Day 9 placeholder] remove_orphan_files older_than 3d "
            "(Day 10 또는 Phase 2 본격 활성)'"
        ),
    )

    snapshot_metrics_after = PythonOperator(
        task_id="snapshot_metrics_after",
        python_callable=_capture_metrics,
        op_kwargs={"table": SILVER_TABLE},
    )

    post_compaction_report = PythonOperator(
        task_id="post_compaction_report",
        python_callable=send_compaction_report,
    )

    (
        snapshot_metrics_before
        >> rewrite
        >> expire_snapshots
        >> remove_orphan_files
        >> snapshot_metrics_after
        >> post_compaction_report
    )
