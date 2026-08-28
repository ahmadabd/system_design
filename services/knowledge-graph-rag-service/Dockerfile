FROM python:3.11-slim

WORKDIR /app

# Install system build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY services/knowledge-graph-rag-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download FastEmbed BGE model to prevent cold start latency
RUN python -c "from fastembed import TextEmbedding; TextEmbedding(model_name='BAAI/bge-small-en-v1.5')" || true

COPY shared/ /app/shared/
COPY services/knowledge-graph-rag-service/src /app/src

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

EXPOSE 8011

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8011"]
