FROM python:3.11-slim

WORKDIR /app

COPY services/webhook-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared monorepo components
COPY shared/ /app/shared/

# Copy service-specific source code
COPY services/webhook-service/src/ /app/src/

# Ensure python can locate shared packages
ENV PYTHONPATH=/app

EXPOSE 8006

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8006"]
