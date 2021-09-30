FROM node:12.22.6-alpine3.12

ARG IMAGE_VERSION=dev

ENV CUBEJS_DOCKER_IMAGE_VERSION=$IMAGE_VERSION
ENV CUBEJS_DOCKER_IMAGE_TAG=dev
ENV CI=0

RUN apk add rxvt-unicode

ENV CUBESTORE_SKIP_POST_INSTALL=true
ENV TERM rxvt-unicode
ENV NODE_ENV development

WORKDIR /cubejs

COPY package.json .
COPY lerna.json .
COPY yarn.lock .
COPY tsconfig.base.json .
COPY rollup.config.js .
COPY packages/cubejs-linter packages/cubejs-linter

# Backend
COPY rust/package.json rust/package.json
COPY rust/bin rust/bin
COPY packages/cubejs-backend-shared/package.json packages/cubejs-backend-shared/package.json
COPY packages/cubejs-testing/package.json packages/cubejs-testing/package.json
COPY packages/cubejs-backend-cloud/package.json packages/cubejs-backend-cloud/package.json
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
# Frontend
COPY packages/cubejs-templates/package.json packages/cubejs-templates/package.json
COPY packages/cubejs-client-core/package.json packages/cubejs-client-core/package.json
COPY packages/cubejs-client-react/package.json packages/cubejs-client-react/package.json
COPY packages/cubejs-client-vue/package.json packages/cubejs-client-vue/package.json
COPY packages/cubejs-client-vue3/package.json packages/cubejs-client-vue3/package.json
COPY packages/cubejs-client-ngx/package.json packages/cubejs-client-ngx/package.json
COPY packages/cubejs-client-ws-transport/package.json packages/cubejs-client-ws-transport/package.json
COPY packages/cubejs-playground/package.json packages/cubejs-playground/package.json

RUN yarn policies set-version v1.22.5

# There is a problem with release process.
# We are doing version bump without updating lock files for the docker package.
#RUN yarn install --frozen-lockfile
RUN yarn install

# Backend
COPY rust/ rust/
COPY packages/cubejs-backend-shared/ packages/cubejs-backend-shared/
COPY packages/cubejs-testing/ packages/cubejs-testing/
COPY packages/cubejs-backend-cloud/ packages/cubejs-backend-cloud/
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
# Frontend
COPY packages/cubejs-templates/ packages/cubejs-templates/
COPY packages/cubejs-client-core/ packages/cubejs-client-core/
COPY packages/cubejs-client-react/ packages/cubejs-client-react/
COPY packages/cubejs-client-vue/ packages/cubejs-client-vue/
COPY packages/cubejs-client-vue3/ packages/cubejs-client-vue3/
COPY packages/cubejs-client-ngx/ packages/cubejs-client-ngx/
COPY packages/cubejs-client-ws-transport/ packages/cubejs-client-ws-transport/
COPY packages/cubejs-playground/ packages/cubejs-playground/

RUN yarn build
RUN yarn lerna run build

COPY packages/cubejs-docker/bin/cubejs-dev /usr/local/bin/cubejs

# By default Node dont search in parent directory from /cube/conf, @todo Reaserch a little bit more
ENV NODE_PATH /cube/conf/node_modules:/cube/node_modules
RUN ln -s  /cubejs/packages/cubejs-docker /cube
RUN ln -s  /cubejs/rust/bin/cubestore-dev /usr/local/bin/cubestore-dev

WORKDIR /cube/conf

EXPOSE 4000

CMD ["cubejs", "server"]
