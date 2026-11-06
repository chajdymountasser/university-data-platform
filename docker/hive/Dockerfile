FROM apache/hive:3.1.3

USER root

# ── Install wget and required tools ────────────────────────────
RUN apt-get update && apt-get install -y wget postgresql-client && rm -rf /var/lib/apt/lists/*

# ── Install PostgreSQL JDBC driver for metastore DB ────────────
RUN wget -q -P /opt/hive/lib/ \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar

# ── Hadoop S3A JARs (compatible with Hive 3.1.3 / Hadoop 3.1.0) ─
RUN wget -q -P /opt/hive/lib/ \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.0/hadoop-aws-3.1.0.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar

# ── Hive configuration ───────────────────────────────────────
COPY hive-site.xml /opt/hive/conf/hive-site.xml
RUN chmod 644 /opt/hive/conf/hive-site.xml

# ── Hadoop S3A config for Hive Metastore (MinIO access) ────
COPY core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
RUN chmod 644 /opt/hadoop/etc/hadoop/core-site.xml

# ── Entrypoint script (waits for DB, inits schema, starts service) ─
COPY entrypoint.sh /opt/hive/bin/entrypoint.sh
RUN chmod 755 /opt/hive/bin/entrypoint.sh

USER hive
EXPOSE 9083

ENTRYPOINT ["/opt/hive/bin/entrypoint.sh"]
