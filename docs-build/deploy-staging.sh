#!/bin/bash

echo PATH_PREFIX=$PATH_PREFIX > .env.production
echo ALGOLIA_API_KEY=$ALGOLIA_API_KEY >> .env.production
echo ALGOLIA_INDEX_NAME=$ALGOLIA_INDEX_NAME >> .env.production

/bin/bash ./build.sh \
&& aws s3 sync public/ s3://cubejs-docs-staging/docs \
&& echo "Deployed staging at: http://cubejs-docs-staging.s3-website-us-east-1.amazonaws.com/docs/"
