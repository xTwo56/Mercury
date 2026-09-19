#!/bin/sh
set -eu

if [ -z "${MERCURY_DATABASE_URL:-}" ]; then
    echo "MERCURY_DATABASE_URL is required" >&2
    exit 1
fi

exec /usr/local/bin/migrate -path /migrations -database "$MERCURY_DATABASE_URL" up
