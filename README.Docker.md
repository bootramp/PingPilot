# PingPilot Web Docker package

This package runs PingPilot Web as a self-contained Linux container. It
includes Python, Gunicorn, nmap, nslookup (dnsutils), and ICMP ping.
Nginx is not required: publish the port you want directly to the container.

## Build

Run this from the pingpilot-web directory:

    docker build -t pingpilot-web:latest .

## Run

Only choose the host port. For example, to use port 219:

    docker run -d --name pingpilot-web --restart unless-stopped -p 8219:8219 --cap-add=NET_RAW -v pingpilot_data:/data pingpilot-web:latest

Open http://SERVER-IP:8219.

For Docker installations that remove the default NET_RAW capability, add
--cap-add=NET_RAW so ICMP/Ping targets can work.

## Docker Compose

    PINGPILOT_PORT=8219 docker compose up -d --build

## Transfer as one image file

Build the image once on any Docker machine, then export it:

    docker save -o pingpilot-web.tar pingpilot-web:latest

Copy pingpilot-web.tar to another server and load it there:

    docker load -i pingpilot-web.tar
    docker run -d --name pingpilot-web --restart unless-stopped -p 8219:8219 --cap-add=NET_RAW -v pingpilot_data:/data pingpilot-web:latest

## Data and secrets

The image deliberately contains no imported monitoring targets, history,
Rocket.Chat configuration, Rocket.Chat users/channels/rooms, or encryption
key. These are created at runtime in the mounted /data volume.

Global Connectivity defaults are source-defined and are created automatically
only when /data has no Global Connectivity endpoints. They remain available
in a clean deployment without including private user data in the image.

To move an existing installation, move or back up the Docker volume
pingpilot_data; do not copy it into the image.

## Upgrade

    docker build -t pingpilot-web:latest .
    docker rm -f pingpilot-web
    docker run -d --name pingpilot-web --restart unless-stopped -p 8219:8219 --cap-add=NET_RAW -v pingpilot_data:/data pingpilot-web:latest

The named volume preserves runtime state across image upgrades.
