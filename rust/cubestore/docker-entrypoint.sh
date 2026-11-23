#!/bin/sh
set -e

# Transition period: older images ran as root, so existing volumes are root-owned
# and get chowned to cubestore on start. From 2027-05-23, drop this script and
# switch to the USER directive in the Dockerfile.
if [ "$(id -u)" = "0" ]; then
  for dir in "${CUBESTORE_DATA_DIR:-/cube/.cubestore/data}" "$CUBESTORE_REMOTE_DIR" "$CUBESTORE_DUMP_DIR"; do
    [ -n "$dir" ] || continue
    mkdir -p "$dir"
    find "$dir" ! -user cubestore -exec chown -h cubestore:cubestore {} +
  done

  exec setpriv --reuid=cubestore --regid=cubestore --init-groups -- "$@"
fi

exec "$@"
