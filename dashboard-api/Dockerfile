FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY app/ app/

# Expose port
EXPOSE 8000

# Unbuffered: stdout is block-buffered on a pipe, so without this the last lines
# before a crash never reach the container log -- exactly when they matter most.
ENV PYTHONUNBUFFERED=1

# Run
# --workers: a single worker serialized all traffic behind whichever request
# held the event loop. Each worker keeps its own DB pool, so total connections
# are workers * DB_POOL_MAX.
# --proxy-headers/--forwarded-allow-ips: otherwise every access line records the
# Traefik container as the client. Safe here -- Traefik is the only route in.
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", \
     "--workers", "4", "--proxy-headers", "--forwarded-allow-ips", "*"]
