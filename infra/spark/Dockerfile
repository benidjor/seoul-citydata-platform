FROM apache/spark:3.5.3-python3

USER root
RUN mkdir -p /opt/spark/extra-jars && \
    cd /opt/spark/extra-jars && \
    curl -fSLO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar && \
    # iceberg-aws-bundle = AWS SDK v2 (S3FileIO 의존). Plan SoT 누락 사후 보강 (Deviation 9.1-B).
    curl -fSLO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar && \
    curl -fSLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fSLO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

USER spark
WORKDIR /workspace
