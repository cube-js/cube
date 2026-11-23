#!/bin/sh
set -e

# Older images ran Cube Store as root, so existing volumes, PVCs and host
# bind mounts are owned by root. When started as root, hand the data directories
# over to the cubestore user and drop privileges, so upgrades keep working.
# If that is not possible (capabilities dropped, root squashing on NFS,
# read-only mounts), keep running as root like older images did.
if [ "$(id -u)" = "0" ]; then
  if ! setpriv --reuid=cubestore --regid=cubestore --init-groups -- true 2>/dev/null; then
    echo "Warning: unable to switch to the cubestore user, running as root" >&2
    exec "$@"
  fi

  migrated=1
  for dir in "${CUBESTORE_DATA_DIR:-/cube/.cubestore/data}" "$CUBESTORE_REMOTE_DIR" "$CUBESTORE_DUMP_DIR"; do
    [ -n "$dir" ] || continue
    mkdir -p "$dir" 2>/dev/null || true
    [ -d "$dir" ] || continue
    # The directory itself is chowned last, so its owner marks a finished
    # migration and later starts skip walking a possibly large tree.
    if [ "$(stat -c %u "$dir")" != "$(id -u cubestore)" ]; then
      echo "Changing ownership of $dir to cubestore"
      if ! { find "$dir" -mindepth 1 ! -user cubestore -exec chown -h cubestore:cubestore {} + \
             && chown cubestore:cubestore "$dir"; }; then
        echo "Warning: unable to change ownership of $dir" >&2
        migrated=0
      fi
    fi
  done

  if [ "$migrated" = "0" ]; then
    echo "Warning: running as root because data directories are not owned by cubestore" >&2
    exec "$@"
  fi

  exec setpriv --reuid=cubestore --regid=cubestore --init-groups -- "$@"
fi

exec "$@"
