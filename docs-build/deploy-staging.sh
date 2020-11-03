#!/bin/bash

/bin/bash ./build.sh \
&& aws s3 sync public/ s3://cubejs-docs-staging/docs \
&& echo "Deployed staging at: http://cubejs-docs-staging.s3-website-us-east-1.amazonaws.com/docs/"
