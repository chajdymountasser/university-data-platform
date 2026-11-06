FROM trinodb/trino:latest

USER root
COPY catalog/ /etc/trino/catalog/

# Install Hudi connector plugin
RUN mkdir -p /usr/lib/trino/plugin/hudi && \
    curl -fsSL -o /usr/lib/trino/plugin/hudi/trino-hudi-481.jar \
    https://repo1.maven.org/maven2/io/trino/trino-hudi/481/trino-hudi-481.jar

USER trino
