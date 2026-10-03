# PingPilot Web

PingPilot Web is a self-hosted monitoring dashboard for Linux. It monitors ICMP, TCP, HTTP/HTTPS, DNS and other configured targets, provides live operational dashboards, Global Connectivity checks, network tools, reports, and Rocket.Chat notifications.

## Highlights

- Live monitoring with target availability, latency, outage timeline, protocol radar and reporting.
- Per-target Rocket.Chat alerts for outage and recovery, plus scheduled connectivity summaries.
- Rocket.Chat delivery to multiple channels, rooms and direct-message recipients.
- CSV import/export, HTML reports, drag-reordering and automatic performance-aware sorting.
- Global Connectivity dashboard with default representative country endpoints.
- Built-in `nslookup` and `nmap` tools.
- Docker image includes Python, Gunicorn, `nmap`, `dnsutils` and ICMP `ping`.

## Run from Docker Hub

```bash
docker pull bootramp/pingpilot-web:latest
docker run -d \
  --name pingpilot-web \
  --restart unless-stopped \
  -p 219:8219 \
  -v pingpilot_data:/data \
  bootramp/pingpilot-web:latest
```

Open `http://SERVER-IP:219`.

If a hardened Docker installation strips ICMP permissions, append `--cap-add=NET_RAW` to the `docker run` command.

## Build locally

```bash
docker build -t pingpilot-web:latest .
docker compose up -d --build
```

Set `PINGPILOT_PORT` before using Compose when a port other than `219` is needed.

## Persistent data and privacy

Runtime state is written only to the mounted `/data` volume. This includes imported targets, check history, Rocket.Chat credentials/destinations and the local encryption key.

Those files are intentionally excluded from this repository and the Docker image. A clean container starts with no personal targets or Rocket.Chat settings. The Global Connectivity default endpoints are part of the application source and are provisioned automatically for a clean `/data` volume.

## Bare-metal deployment

The `deploy/` directory contains example `systemd` and Nginx configuration for a non-Docker Linux deployment. Adjust paths, service account and listener settings to match the host before enabling them.

## Documentation

For image-transfer and upgrade instructions, see [README.Docker.md](README.Docker.md).
