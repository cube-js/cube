#!/usr/bin/env bash
# Upload a static asset to the cube-dev-websites-shared S3 bucket and print
# the resulting https://static.cube.dev/<key> URL.
#
# Usage:
#   ./scripts/upload-asset.sh <local-file> <dest-key> [--force]
#
# Examples:
#   ./scripts/upload-asset.sh ./snowflake.svg icons/snowflake.svg
#   ./scripts/upload-asset.sh ./architecture.png docs/getting-started/architecture.png
#   ./scripts/upload-asset.sh ./diagram.svg     diagrams/pre-aggregations-flow.svg
#
# Conventions (see scripts/README.md):
#   icons/<slug>.{svg,png}                          — provider / integration logos
#   docs/<section>/<slug>/<filename>                — screenshots & images per page
#   diagrams/<slug>.{svg,png}                       — architecture / flow diagrams
#   recipes/<slug>/<filename>                       — recipe-specific assets
#
# Paths are meant to be IMMUTABLE. If you need to change an image, upload a
# new key (e.g. logo-v2.svg) and update the Markdown reference.

set -euo pipefail

BUCKET="cube-dev-websites-shared"
REGION="us-west-2"
PUBLIC_BASE="https://static.cube.dev"
PROFILE="${AWS_PROFILE:-cube-static}"

usage() {
  sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
  exit 1
}

if [[ $# -lt 2 ]]; then
  usage
fi

SRC="$1"
KEY="$2"
FORCE="${3:-}"

if [[ ! -f "$SRC" ]]; then
  echo "error: source file not found: $SRC" >&2
  exit 1
fi

if [[ "$KEY" == /* || "$KEY" == *..* ]]; then
  echo "error: dest key must be a relative path without '..': $KEY" >&2
  exit 1
fi

if ! command -v aws >/dev/null 2>&1; then
  echo "error: aws CLI is not installed. Run: brew install awscli" >&2
  exit 1
fi

guess_content_type() {
  local f
  f="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')"
  case "$f" in
    *.svg)          echo "image/svg+xml" ;;
    *.png)          echo "image/png" ;;
    *.jpg|*.jpeg)   echo "image/jpeg" ;;
    *.gif)          echo "image/gif" ;;
    *.webp)         echo "image/webp" ;;
    *.avif)         echo "image/avif" ;;
    *.ico)          echo "image/x-icon" ;;
    *.mp4)          echo "video/mp4" ;;
    *.webm)         echo "video/webm" ;;
    *.pdf)          echo "application/pdf" ;;
    *.json)         echo "application/json" ;;
    *.txt|*.md)     echo "text/plain; charset=utf-8" ;;
    *)              file --mime-type -b "$1" 2>/dev/null || echo "application/octet-stream" ;;
  esac
}

CONTENT_TYPE="$(guess_content_type "$SRC")"
CACHE_CONTROL="public, max-age=31536000, immutable"

echo "→ bucket:       s3://${BUCKET}/${KEY}"
echo "→ region:       ${REGION}"
echo "→ profile:      ${PROFILE}"
echo "→ content-type: ${CONTENT_TYPE}"
echo "→ cache:        ${CACHE_CONTROL}"
echo

# Refuse to overwrite unless --force is passed.
if [[ "$FORCE" != "--force" ]]; then
  if aws s3api head-object \
       --bucket "$BUCKET" \
       --key "$KEY" \
       --region "$REGION" \
       --profile "$PROFILE" >/dev/null 2>&1; then
    echo "error: key already exists in bucket: ${KEY}" >&2
    echo "  Our convention is immutable paths. Pick a new key (e.g. add -v2)," >&2
    echo "  or pass --force if you really mean to overwrite." >&2
    exit 1
  fi
fi

aws s3 cp "$SRC" "s3://${BUCKET}/${KEY}" \
  --region "$REGION" \
  --profile "$PROFILE" \
  --content-type "$CONTENT_TYPE" \
  --cache-control "$CACHE_CONTROL"

URL="${PUBLIC_BASE}/${KEY}"
echo
echo "✓ uploaded"
echo "  ${URL}"

# Copy URL to clipboard on macOS.
if command -v pbcopy >/dev/null 2>&1; then
  printf '%s' "$URL" | pbcopy
  echo "  (copied to clipboard)"
fi
