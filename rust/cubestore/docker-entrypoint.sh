#!/bin/sh
set -e

# Older images ran as root, so existing volumes are root-owned: chown them to
# cubestore and drop privileges, or keep running as root when that is impossible.
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
      # Nodes sharing a directory migrate it concurrently, so entries can vanish
      # mid-walk. The walk's status is ignored and leftovers decide the outcome.
      find "$dir" -mindepth 1 ! -user cubestore -exec chown -h cubestore:cubestore {} + || true
      if ! chown cubestore:cubestore "$dir" \
         || [ -n "$(find "$dir" ! -user cubestore -print -quit 2>/dev/null)" ]; then
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
