FROM python:3.11-slim

WORKDIR /app

ARG BIFROST_CORE_REF=main
ARG GITHUB_ORG=YOUR_ORG

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev git \
    && rm -rf /var/lib/apt/lists/*

# Install shared library from bifrost-trade-core
RUN pip install --no-cache-dir \
    "bifrost-core @ git+https://github.com/${GITHUB_ORG}/bifrost-trade-core.git@${BIFROST_CORE_REF}"

COPY pyproject.toml .
COPY src/ src/
RUN pip install --no-cache-dir -e "."

COPY scripts/ scripts/

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Override CMD per service in docker-compose
CMD ["python", "scripts/run_ib_ingestor.py"]
