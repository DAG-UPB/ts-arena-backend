FROM python:3.12-slim

WORKDIR /service

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt && pip install --no-cache-dir "apscheduler>=4.0.0a1"

COPY app/ ./app/

ENV PYTHONUNBUFFERED=1

# --proxy-headers/--forwarded-allow-ips: without these the access log records the
# Traefik container (172.18.0.x) as the client for every request, so scan traffic
# cannot be attributed to a source. Traefik already injects X-Forwarded-For; uvicorn
# just has to be told to read it. `*` is safe here because the only route to this
# container is Traefik -- no host port is published.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", \
     "--proxy-headers", "--forwarded-allow-ips", "*"]