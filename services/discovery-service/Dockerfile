FROM python:3.11-slim

WORKDIR /app

# Install system curl and build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY services/discovery-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ /app/shared/
COPY services/discovery-service/src/ /app/src/

ENV PYTHONPATH=/app
ENV PORT=8009

EXPOSE 8009

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8009"]
