#!/bin/bash

# Exit on first error
set -e

if [ $# -eq 0 ]; then
    echo "Please specify the path to image."
    exit 1
fi

S3_BUCKET=${2:-cubedev-blog-images}
S3_PATH=${2:-s3://$S3_BUCKET/}
IMAGE_PATH=$1
IMAGE_NAME="${IMAGE_PATH##*/}"
IMAGE_EXTENSION=${IMAGE_PATH##*.}
UUID=`echo "$(uuidgen)" | tr '[:upper:]' '[:lower:]'`
NEW_IMAGE_PATH="$S3_PATH$UUID.$IMAGE_EXTENSION"
NEW_IMAGE_URL="https://$S3_BUCKET.s3.us-east-2.amazonaws.com/$UUID.$IMAGE_EXTENSION"

aws s3 cp --acl public-read "$IMAGE_PATH" "$NEW_IMAGE_PATH"

echo ""
echo "Uploaded: $NEW_IMAGE_URL"

echo ""
echo "Markdown: ![$IMAGE_NAME]($NEW_IMAGE_URL)"
