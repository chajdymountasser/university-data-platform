FROM apache/airflow:2.9.1

USER root

RUN apt-get update -qq && \
    apt-get install -y -qq --no-install-recommends \
        libglib2.0-0 libnss3 libnspr4 libatk-bridge2.0-0 \
        libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 \
        libxrandr2 libgbm1 libpango-1.0-0 libcairo2 \
        libasound2 libatk1.0-0 libcups2 libdbus-1-3 \
        libxfixes3 libxtst6 libxshmfence1 libegl1 \
        libxcb-dri3-0 libxcb-present0 libxcb-randr0 \
        libxcb-xfixes0 libxcb-shape0 libxcb-sync1 \
        libxxf86vm1 \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# ── Core ingestion & platform dependencies (baked in, not at runtime) ──
RUN pip install --no-cache-dir \
    boto3>=1.34.0 \
    requests>=2.31.0 \
    beautifulsoup4>=4.12.0 \
    pydantic>=2.0.0 \
    python-dotenv>=1.0.0 \
    typer>=0.9.0 \
    httpx>=0.27.0 \
    pyyaml>=6.0 \
    "numpy>=1.24.0,<2.0.0" \
    apache-airflow-providers-postgres \
    playwright>=1.42.0 \
    qdrant-client>=1.9.0 \
    elasticsearch>=8.12.0 \
    sentence-transformers>=3.0.0 \
    torch>=2.0.0

RUN python3 -m playwright install chromium
