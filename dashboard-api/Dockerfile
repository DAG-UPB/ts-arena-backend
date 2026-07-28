FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY app/ app/

# Expose port
EXPOSE 8000

# Run
# --workers: a single worker serialized all traffic behind whichever request
# held the event loop. Each worker keeps its own DB pool, so total connections
# are workers * DB_POOL_MAX.
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
