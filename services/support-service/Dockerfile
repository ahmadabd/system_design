FROM python:3.11-slim

WORKDIR /app

# Install build dependencies if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY services/support-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared monorepo components
COPY shared/ /app/shared/

# Copy knowledge base documents
COPY services/support-service/knowledge_base/ /app/knowledge_base/

# Copy benchmark evaluation suite
COPY services/support-service/eval/ /app/eval/

# Copy service-specific source code
COPY services/support-service/src/ /app/src/


# Ensure python can locate shared packages
ENV PYTHONPATH=/app

EXPOSE 8007

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8007"]
