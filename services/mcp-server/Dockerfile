FROM python:3.11-slim

WORKDIR /app

COPY services/mcp-server/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared monorepo components
COPY shared/ /app/shared/
COPY algorithms/ /app/algorithms/

# Copy service-specific source code
COPY services/mcp-server/src/ /app/src/

# Ensure python can locate shared packages
ENV PYTHONPATH=/app

EXPOSE 8008

CMD ["python", "-m", "src.main"]
