FROM python:3.11-slim

WORKDIR /app

COPY services/payment-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared monorepo components
COPY shared/ /app/shared/

# Copy service-specific source code
# Copy Alembic migrations configuration and files
COPY services/payment-service/alembic.ini /app/alembic.ini
COPY services/payment-service/migrations/ /app/migrations/

# Copy service-specific source code
COPY services/payment-service/src/ /app/src/

# Ensure python can locate shared packages
ENV PYTHONPATH=/app

EXPOSE 8004

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8004"]
