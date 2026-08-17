FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/

# Create non-root user
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Run the application
# Unbuffered: stdout is block-buffered on a pipe, so without this the last lines
# before a crash never reach the container log -- exactly when they matter most.
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "src.main"]
