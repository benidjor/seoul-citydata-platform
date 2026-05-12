# Day 9 Spark Runbook

Day 9 는 Spark 의 일시 기동 batch 작업 = MERGE INTO 멱등성 + `rewrite_data_files` Compaction. Kafka + PyFlink + Airflow 가 상시 가동되는 24GB 환경에서 OOM 회피를 위한 절차 명문화.

## 일시 기동 절차 (메모리 mitigation 포함)

```bash
# 1. airflow-scheduler 일시 stop — 약 700MB 회수 (spec §9-3 + airflow-decision 메모리 SoT)
docker compose stop airflow-scheduler
free -h   # 80% 임계 (19.2GB) 안 확인 (macOS 는 vm_stat)

# 2. Spark profile 일시 기동
docker compose --profile spark up -d spark

# 3. batch job 실행
docker compose exec -T spark /opt/spark/bin/spark-submit /workspace/jobs/merge_dim_place.py
docker compose exec -T spark /opt/spark/bin/spark-submit /workspace/jobs/compaction_silver.py

# 4. Spark 일시 down + airflow-scheduler 재기동
docker compose rm -sf spark           # PR #53 의 Deviation 9.1-D 정공 명령
docker compose start airflow-scheduler

# 5. baseline 복귀 확인
bash scripts/healthcheck.sh           # failed sections 0 의무
```

### Deviation 9.1-D 의 정공 명령 SoT (PR #53 archive)

Day 9 Spark 일시 down 명령은 profile 매개변수의 의도와 다르게 동작한다. 정공/오공 비교:

| 명령 | 결과 |
|---|---|
| `docker compose --profile spark down` (오공) | 전체 stack 을 down 시킴 (profile 미지정 service 까지 포함) |
| `docker compose rm -sf spark` (정공) | spark 만 down + 볼륨 정리. baseline 보존 |
| `docker compose stop scp-spark` (대안) | spark 만 stop (다음에 다시 start 가능, 다만 일시 기동 의도엔 rm 이 더 깔끔) |

## Spark 컨테이너 사양

- driver 1g + executor 1g (local mode)
- jar 4종 (`iceberg-spark-runtime-3.5_2.12-1.7.1` + `iceberg-aws-bundle-1.7.1` + `hadoop-aws-3.3.4` + `aws-java-sdk-bundle-1.12.262`)
- profile=spark — `docker compose up -d` 평상시 미기동
- volumes: `infra/spark/conf` (read-only) + `infra/spark/jobs` (read-only)

## 산출물 (검증 명령)

| 검증 | 명령 | 기대 |
|---|---|---|
| MERGE 멱등성 | `spark-submit /workspace/jobs/merge_dim_place.py` | `OK: idempotent (rows + content hash 동일)` |
| Compaction | `spark-submit /workspace/jobs/compaction_silver.py` | `file reduction: 50%+` + `t_after < t_before` |
| 비용 모델 | `uv run python infra/spark/jobs/cost_report.py` | `TOTAL : 0.83` (도메인 사용 시) |

## 1번 프로젝트 (레시핑) 미해결 closure 매핑

| 레시핑의 미해결 (페이지 9·11) | 본 Day 9 의 증거 |
|---|---|
| Dynamic Partition Overwrite "예정" | `MERGE INTO` 멱등성 검증 (rows + content hash 동일) — PR #53 |
| Compaction "도입 예정" | `rewrite_data_files` before/after 파일 수 50%+ 감소 — 본 PR |

## Fallback (spec §9-1 Day 9)

Spark 셋업 4시간 초과 시:

- PyIceberg 로 dedup + read 검증
- dbt-duckdb `incremental` + `merge` 전략으로 멱등성 검증
- portfolio 운영 메모에 "Spark 시도 → 우회" 사실 기록
