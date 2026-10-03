#!/bin/sh
set -eu

DATA_DIR="${PINGPILOT_DATA:-/data}"
mkdir -p "$DATA_DIR"

# Works for Docker named volumes and common bind mounts. The state directory
# is deliberately outside /app, so image upgrades do not overwrite it.
chown -R pingpilot:pingpilot "$DATA_DIR"
chmod 700 "$DATA_DIR"

exec gosu pingpilot "$@"
