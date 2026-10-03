FROM python:3.13-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PINGPILOT_DATA=/data

WORKDIR /app

# iputils-ping powers ICMP targets, dnsutils provides nslookup, and nmap
# powers the controlled Network Tools scan page.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        dnsutils \
        gosu \
        iputils-ping \
        libcap2-bin \
        nmap \
    && (setcap cap_net_raw+ep /usr/bin/ping || true) \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py ./
COPY static/ ./static/
COPY templates/ ./templates/
COPY docker-entrypoint.sh /usr/local/bin/pingpilot-entrypoint

RUN chmod 0755 /usr/local/bin/pingpilot-entrypoint \
    && groupadd --system --gid 10001 pingpilot \
    && useradd --system --uid 10001 --gid pingpilot --create-home --home-dir /app pingpilot \
    && mkdir -p /data \
    && chown -R pingpilot:pingpilot /app /data

VOLUME ["/data"]
EXPOSE 8219

HEALTHCHECK --interval=30s --timeout=8s --start-period=20s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8219/api/state', timeout=5).read()" || exit 1

ENTRYPOINT ["/usr/local/bin/pingpilot-entrypoint"]
CMD ["gunicorn", "--workers", "1", "--threads", "8", "--bind", "0.0.0.0:8219", "--timeout", "120", "--access-logfile", "-", "--error-logfile", "-", "app:app"]
