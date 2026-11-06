FROM apache/spark:3.5.3

USER root

RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

# ── Hudi bundle (Spark 3.5, Scala 2.12) ──────────────────────
RUN wget -q -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.5-bundle_2.12/0.15.0/hudi-spark3.5-bundle_2.12-0.15.0.jar

# ── Hadoop AWS + AWS SDK (s3a:// connectivity) ────────────────
RUN wget -q -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# ── Hive Metastore client JAR for Thrift connectivity ────────
RUN wget -q -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/hive/hive-metastore/3.1.3/hive-metastore-3.1.3.jar \
    https://repo1.maven.org/maven2/org/apache/thrift/libthrift/0.19.0/libthrift-0.19.0.jar

# ── Spark configuration ───────────────────────────────────────
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY core-site.xml /opt/spark/conf/core-site.xml
COPY hive-site.xml /opt/spark/conf/hive-site.xml

# ── Python dependencies ──
RUN pip install --no-cache-dir \
    beautifulsoup4>=4.12.0 \
    trafilatura>=1.12.0 \
    "numpy>=1.24.0,<2.0.0" \
    PyMuPDF>=1.23.0 \
    python-dotenv>=1.0.0 \
    sentence-transformers>=3.0.0 \
    torch>=2.0.0

# ── Workspace mount point for pipeline scripts ───────────────
RUN mkdir -p /opt/spark/apps && chmod 755 /opt/spark/apps

ENV PATH="/opt/spark/bin:${PATH}"

USER spark
WORKDIR /opt/spark
