FROM node:12.20.1-alpine

ARG IMAGE_VERSION=dev

ENV CUBEJS_DOCKER_IMAGE_VERSION=$IMAGE_VERSION
ENV CUBEJS_DOCKER_IMAGE_TAG=dev
ENV CI=0

RUN apk add rxvt-unicode

# For now Cube.js docker image is building without waiting cross jobs, it's why we are not able to install it
ENV CUBESTORE_SKIP_POST_INSTALL=true
ENV TERM rxvt-unicode
ENV NODE_ENV production

WORKDIR /cubejs

COPY package.json .
COPY lerna.json .
COPY yarn.lock .
COPY tsconfig.base.json .
COPY packages/cubejs-linter packages/cubejs-linter

# There is a problem with release process.
# We are doing version bump without updating lock files for the docker package.
#RUN yarn install --frozen-lockfile
RUN yarn install

# @todo https://stackoverflow.com/questions/49939960/docker-copy-files-using-glob-pattern/50010093
#COPY packages/*/package.json ./
#COPY packages/*/yarn.lock ./

# Attention, playground/client/serverless are ignored for now!
# ls  | awk '{ print "COPY packages/" $1 "/package.json packages/" $1 "/package.json"}'
# ls  | awk '{ print "COPY packages/" $1 "/yarn.lock packages/" $1 "/yarn.lock"}'

COPY rust/package.json rust/package.json
COPY packages/cubejs-backend-shared/package.json packages/cubejs-backend-shared/package.json
COPY packages/cubejs-api-gateway/package.json packages/cubejs-api-gateway/package.json
COPY packages/cubejs-athena-driver/package.json packages/cubejs-athena-driver/package.json
COPY packages/cubejs-bigquery-driver/package.json packages/cubejs-bigquery-driver/package.json
COPY packages/cubejs-cli/package.json packages/cubejs-cli/package.json
COPY packages/cubejs-clickhouse-driver/package.json packages/cubejs-clickhouse-driver/package.json
COPY packages/cubejs-docker/package.json packages/cubejs-docker/package.json
COPY packages/cubejs-dremio-driver/package.json packages/cubejs-dremio-driver/package.json
COPY packages/cubejs-druid-driver/package.json packages/cubejs-druid-driver/package.json
COPY packages/cubejs-elasticsearch-driver/package.json packages/cubejs-elasticsearch-driver/package.json
COPY packages/cubejs-hive-driver/package.json packages/cubejs-hive-driver/package.json
COPY packages/cubejs-mongobi-driver/package.json packages/cubejs-mongobi-driver/package.json
COPY packages/cubejs-mssql-driver/package.json packages/cubejs-mssql-driver/package.json
COPY packages/cubejs-mysql-driver/package.json packages/cubejs-mysql-driver/package.json
COPY packages/cubejs-cubestore-driver/package.json packages/cubejs-cubestore-driver/package.json
COPY packages/cubejs-oracle-driver/package.json packages/cubejs-oracle-driver/package.json
COPY packages/cubejs-postgres-driver/package.json packages/cubejs-postgres-driver/package.json
COPY packages/cubejs-prestodb-driver/package.json packages/cubejs-prestodb-driver/package.json
COPY packages/cubejs-query-orchestrator/package.json packages/cubejs-query-orchestrator/package.json
COPY packages/cubejs-schema-compiler/package.json packages/cubejs-schema-compiler/package.json
COPY packages/cubejs-server/package.json packages/cubejs-server/package.json
COPY packages/cubejs-server-core/package.json packages/cubejs-server-core/package.json
COPY packages/cubejs-snowflake-driver/package.json packages/cubejs-snowflake-driver/package.json
COPY packages/cubejs-sqlite-driver/package.json packages/cubejs-sqlite-driver/package.json

COPY rust/yarn.lock rust/yarn.lock
COPY packages/cubejs-backend-shared/yarn.lock packages/cubejs-backend-shared/yarn.lock
COPY packages/cubejs-api-gateway/yarn.lock packages/cubejs-api-gateway/yarn.lock
COPY packages/cubejs-athena-driver/yarn.lock packages/cubejs-athena-driver/yarn.lock
COPY packages/cubejs-bigquery-driver/yarn.lock packages/cubejs-bigquery-driver/yarn.lock
COPY packages/cubejs-cli/yarn.lock packages/cubejs-cli/yarn.lock
COPY packages/cubejs-clickhouse-driver/yarn.lock packages/cubejs-clickhouse-driver/yarn.lock
COPY packages/cubejs-docker/yarn.lock packages/cubejs-docker/yarn.lock
COPY packages/cubejs-dremio-driver/yarn.lock packages/cubejs-dremio-driver/yarn.lock
COPY packages/cubejs-druid-driver/yarn.lock packages/cubejs-druid-driver/yarn.lock
COPY packages/cubejs-elasticsearch-driver/yarn.lock packages/cubejs-elasticsearch-driver/yarn.lock
COPY packages/cubejs-hive-driver/yarn.lock packages/cubejs-hive-driver/yarn.lock
COPY packages/cubejs-mongobi-driver/yarn.lock packages/cubejs-mongobi-driver/yarn.lock
COPY packages/cubejs-mssql-driver/yarn.lock packages/cubejs-mssql-driver/yarn.lock
COPY packages/cubejs-mysql-driver/yarn.lock packages/cubejs-mysql-driver/yarn.lock
COPY packages/cubejs-cubestore-driver/yarn.lock packages/cubejs-cubestore-driver/yarn.lock
COPY packages/cubejs-oracle-driver/yarn.lock packages/cubejs-oracle-driver/yarn.lock
COPY packages/cubejs-postgres-driver/yarn.lock packages/cubejs-postgres-driver/yarn.lock
COPY packages/cubejs-prestodb-driver/yarn.lock packages/cubejs-prestodb-driver/yarn.lock
COPY packages/cubejs-query-orchestrator/yarn.lock packages/cubejs-query-orchestrator/yarn.lock
COPY packages/cubejs-schema-compiler/yarn.lock packages/cubejs-schema-compiler/yarn.lock
COPY packages/cubejs-server/yarn.lock packages/cubejs-server/yarn.lock
COPY packages/cubejs-server-core/yarn.lock packages/cubejs-server-core/yarn.lock
COPY packages/cubejs-snowflake-driver/yarn.lock packages/cubejs-snowflake-driver/yarn.lock
COPY packages/cubejs-sqlite-driver/yarn.lock packages/cubejs-sqlite-driver/yarn.lock

#  --ignore @cubejs-backend/jdbc-driver not needed, because it's ignored by .dockerignore
RUN yarn lerna bootstrap

COPY rust/ rust/
COPY packages/cubejs-backend-shared/ packages/cubejs-backend-shared/
COPY packages/cubejs-api-gateway/ packages/cubejs-api-gateway/
COPY packages/cubejs-athena-driver/ packages/cubejs-athena-driver/
COPY packages/cubejs-bigquery-driver/ packages/cubejs-bigquery-driver/
COPY packages/cubejs-cli/ packages/cubejs-cli/
COPY packages/cubejs-clickhouse-driver/ packages/cubejs-clickhouse-driver/
COPY packages/cubejs-docker/ packages/cubejs-docker/
COPY packages/cubejs-dremio-driver/ packages/cubejs-dremio-driver/
COPY packages/cubejs-druid-driver/ packages/cubejs-druid-driver/
COPY packages/cubejs-elasticsearch-driver/ packages/cubejs-elasticsearch-driver/
COPY packages/cubejs-hive-driver/ packages/cubejs-hive-driver/
COPY packages/cubejs-mongobi-driver/ packages/cubejs-mongobi-driver/
COPY packages/cubejs-mssql-driver/ packages/cubejs-mssql-driver/
COPY packages/cubejs-mysql-driver/ packages/cubejs-mysql-driver/
COPY packages/cubejs-cubestore-driver/ packages/cubejs-cubestore-driver/
COPY packages/cubejs-oracle-driver/ packages/cubejs-oracle-driver/
COPY packages/cubejs-postgres-driver/ packages/cubejs-postgres-driver/
COPY packages/cubejs-prestodb-driver/ packages/cubejs-prestodb-driver/
COPY packages/cubejs-query-orchestrator/ packages/cubejs-query-orchestrator/
COPY packages/cubejs-schema-compiler/ packages/cubejs-schema-compiler/
COPY packages/cubejs-server/ packages/cubejs-server/
COPY packages/cubejs-server-core/ packages/cubejs-server-core/
COPY packages/cubejs-snowflake-driver/ packages/cubejs-snowflake-driver/
COPY packages/cubejs-sqlite-driver/ packages/cubejs-sqlite-driver/

RUN yarn lerna run build
COPY packages/cubejs-docker/bin/cubejs-dev /usr/local/bin/cubejs
COPY packages/cubejs-docker/bin/cubestore-dev /usr/local/bin/cubestore-dev

# By default Node dont search in parent directory from /cube/conf, @todo Reaserch a little bit more
ENV NODE_PATH /cube/conf/node_modules:/cube/node_modules
RUN ln -s  /cubejs/packages/cubejs-docker /cube

WORKDIR /cube/conf

EXPOSE 4000

CMD ["cubejs", "server"]
