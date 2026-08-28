FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app:/app/services/merchant-copilot-service

# Install system utilities and curl for healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies
COPY services/merchant-copilot-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN python -c "from fastembed import TextEmbedding; TextEmbedding(model_name='BAAI/bge-small-en-v1.5')" || true

# Copy shared library code and service code
COPY shared /app/shared
COPY services/merchant-copilot-service/src /app/services/merchant-copilot-service/src

WORKDIR /app/services/merchant-copilot-service

EXPOSE 8010

CMD ["python", "-m", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8010"]
