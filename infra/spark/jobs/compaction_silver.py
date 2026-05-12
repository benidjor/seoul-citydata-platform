"""Day 9: silver.hotspot_congestion 의 small file 을 rewrite_data_files 로 압축.

before/after metric 출력. spec §8-1 #2 의 "Compaction 도입 closure" 직접 증거.

PR α (#53) 의 deviation reuse (merge_dim_place.py SoT 와 동일):
- 9.1-A: Lakekeeper warehouse = `seoul` (name)
- 9.1-B: iceberg-aws-bundle (S3FileIO + AWS SDK v2)
- 9.1-C: extraClassPath 절대 경로 (4 jar)
- 9.2-A: 3-part identifier (`ice.silver.hotspot_congestion`, NOT 4-part).
  Lakekeeper REST 안의 actual namespace = flat single-level (bronze/silver/gold).

procedure call argument 형식:
- 1차 시도: `table => 'silver.hotspot_congestion'` (3-part 와 동일 의미, namespace.table)
- spark-submit 1차 시도에서 falsify 후 정공 채택.
"""

from __future__ import annotations

import time

from pyspark.sql import SparkSession


def session() -> SparkSession:
    return SparkSession.builder.appName("scp.day9.compaction").getOrCreate()


def file_count(spark: SparkSession) -> tuple[int, float]:
    df = spark.sql("""
        SELECT count(*) AS n, avg(file_size_in_bytes)/1024/1024.0 AS avg_mb
        FROM ice.silver.hotspot_congestion.files
    """)
    row = df.collect()[0]
    return int(row["n"]), float(row["avg_mb"] or 0.0)


def query_time(spark: SparkSession) -> float:
    start = time.time()
    spark.sql("""
        SELECT district, count(*) c, avg(congest_level_score) s
        FROM ice.silver.hotspot_congestion
        GROUP BY district
    """).collect()
    return time.time() - start


def main() -> None:
    spark = session()

    n_before, mb_before = file_count(spark)
    t_before = query_time(spark)
    print(
        f"before: files={n_before} avg_size_mb={mb_before:.3f} "
        f"group_by_query_seconds={t_before:.2f}"
    )

    spark.sql("""
        CALL ice.system.rewrite_data_files(
            table => 'silver.hotspot_congestion',
            options => map('target-file-size-bytes', '134217728')
        )
    """)

    n_after, mb_after = file_count(spark)
    t_after = query_time(spark)
    print(
        f"after : files={n_after} avg_size_mb={mb_after:.3f} group_by_query_seconds={t_after:.2f}"
    )

    if n_before > 0:
        reduction_pct = 100.0 * (n_before - n_after) / n_before
        print(f"file reduction: {reduction_pct:.1f}%")


if __name__ == "__main__":
    main()
