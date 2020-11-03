#!/bin/bash

/bin/bash ./build.sh \
&& aws s3 sync public/ s3://cubejs/docs \
&& aws cloudfront create-invalidation --distribution-id E32Q4UMUFUPI8O --paths /docs/*
