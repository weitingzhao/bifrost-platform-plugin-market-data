FROM python:3.11-slim-bookworm

WORKDIR /app
RUN pip install --no-cache-dir --upgrade pip

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .

COPY config/market-data.yaml.example /config/market-data.yaml
COPY scripts/run_worker.py ./scripts/run_worker.py

ENV PYTHONUNBUFFERED=1
ENV MARKET_DATA_CONFIG=/config/market-data.yaml

CMD ["python", "scripts/run_worker.py"]
