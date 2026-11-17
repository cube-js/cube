#!/bin/bash
# Waits until Yarn can resolve every public workspace package at the lerna.json version.
# The registry CDN can lag behind `lerna publish` for minutes, and installs of the new
# release then fail with YN0082 "No candidates found".
set -eo pipefail

TIMEOUT_SECONDS=${NPM_WAIT_TIMEOUT_SECONDS:-1800}
INTERVAL_SECONDS=${NPM_WAIT_INTERVAL_SECONDS:-15}

VERSION=$(jq -r .version lerna.json)
PENDING=($(yarn workspaces list --json --no-private | jq -r .name))
echo "Waiting for ${#PENDING[@]} packages at ${VERSION}"

DEADLINE=$((SECONDS + TIMEOUT_SECONDS))

while true; do
  SPECS=()
  for name in "${PENDING[@]}"; do
    SPECS+=("${name}@${VERSION}")
  done

  # `yarn npm info` falls back to the latest version when the requested one is unknown,
  # so availability is checked by the version it returns, not by the exit code
  PENDING=($(
    { yarn npm info "${SPECS[@]}" --fields name,version --json || true; } \
      | jq -rs --arg version "$VERSION" --args '
          (map(select(.version == $version) | .name)) as $found
          | $ARGS.positional - $found | .[]
        ' "${PENDING[@]}"
  ))

  if [ ${#PENDING[@]} -eq 0 ]; then
    echo "All packages are available"
    exit 0
  fi

  if [ $SECONDS -ge $DEADLINE ]; then
    echo "Timed out, still missing at ${VERSION}:"
    printf '  %s\n' "${PENDING[@]}"
    exit 1
  fi

  echo "${#PENDING[@]} packages are not available yet:"
  printf '  %s\n' "${PENDING[@]}"
  sleep "$INTERVAL_SECONDS"
done
