FROM node:18.20.3-bullseye-slim AS build

ARG IMAGE_VERSION=dev

ENV CUBEJS_DOCKER_IMAGE_VERSION=$IMAGE_VERSION
ENV CUBEJS_DOCKER_IMAGE_TAG=dev
ENV CI=0

RUN DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get install -y --no-install-recommends rxvt-unicode libssl1.1 curl \
       cmake python3 libpython3-dev gcc g++ make cmake openjdk-11-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

ENV CUBESTORE_SKIP_POST_INSTALL=true
ENV TERM rxvt-unicode
ENV NODE_ENV development

RUN yarn policies set-version v1.22.19
# Yarn v1 uses aggressive timeouts with summing time spending on fs, https://github.com/yarnpkg/yarn/issues/4890
RUN yarn config set network-timeout 120000 -g

WORKDIR /cubejs

COPY package.json .
COPY lerna.json .
COPY yarn.lock .
COPY tsconfig.base.json .
COPY rollup.config.js .
COPY packages/cubejs-linter packages/cubejs-linter

# Backend
COPY packages/cubejs-backend-shared/ packages/cubejs-backend-shared/
COPY packages/cubejs-base-driver/ packages/cubejs-base-driver/
COPY packages/cubejs-backend-native/ packages/cubejs-backend-native/
COPY packages/cubejs-testing-shared/ packages/cubejs-testing-shared/
COPY packages/cubejs-backend-cloud/ packages/cubejs-backend-cloud/
COPY packages/cubejs-api-gateway/ packages/cubejs-api-gateway/
#COPY packages/cubejs-athena-driver/ packages/cubejs-athena-driver/
#COPY packages/cubejs-bigquery-driver/ packages/cubejs-bigquery-driver/
COPY packages/cubejs-cli/ packages/cubejs-cli/
#COPY packages/cubejs-clickhouse-driver/ packages/cubejs-clickhouse-driver/
#COPY packages/cubejs-crate-driver/ packages/cubejs-crate-driver/
#COPY packages/cubejs-dremio-driver/ packages/cubejs-dremio-driver/
#COPY packages/cubejs-druid-driver/ packages/cubejs-druid-driver/
#COPY packages/cubejs-duckdb-driver/ packages/cubejs-duckdb-driver/
#COPY packages/cubejs-elasticsearch-driver/ packages/cubejs-elasticsearch-driver/
#COPY packages/cubejs-firebolt-driver/ packages/cubejs-firebolt-driver/
#COPY packages/cubejs-hive-driver/ packages/cubejs-hive-driver/
#COPY packages/cubejs-mongobi-driver/ packages/cubejs-mongobi-driver/
#COPY packages/cubejs-mssql-driver/ packages/cubejs-mssql-driver/
#COPY packages/cubejs-mysql-driver/ packages/cubejs-mysql-driver/
COPY packages/cubejs-cubestore-driver/ packages/cubejs-cubestore-driver/
#COPY packages/cubejs-oracle-driver/ packages/cubejs-oracle-driver/
#COPY packages/cubejs-redshift-driver/ packages/cubejs-redshift-driver/
COPY packages/cubejs-postgres-driver/ packages/cubejs-postgres-driver/
#COPY packages/cubejs-questdb-driver/ packages/cubejs-questdb-driver/
#COPY packages/cubejs-materialize-driver/ packages/cubejs-materialize-driver/
#COPY packages/cubejs-prestodb-driver/ packages/cubejs-prestodb-driver/
#COPY packages/cubejs-trino-driver/ packages/cubejs-trino-driver/
COPY packages/cubejs-query-orchestrator/ packages/cubejs-query-orchestrator/
COPY packages/cubejs-schema-compiler/ packages/cubejs-schema-compiler/
COPY packages/cubejs-server/ packages/cubejs-server/
COPY packages/cubejs-server-core/ packages/cubejs-server-core/
#COPY packages/cubejs-snowflake-driver/ packages/cubejs-snowflake-driver/
#COPY packages/cubejs-sqlite-driver/ packages/cubejs-sqlite-driver/
#COPY packages/cubejs-ksql-driver/ packages/cubejs-ksql-driver/
#COPY packages/cubejs-dbt-schema-extension/ packages/cubejs-dbt-schema-extension/
#COPY packages/cubejs-jdbc-driver/ packages/cubejs-jdbc-driver/
#COPY packages/cubejs-databricks-jdbc-driver/ packages/cubejs-databricks-jdbc-driver/
COPY packages/cubejs-event-emitter/ packages/cubejs-event-emitter/
# Skip
# COPY packages/cubejs-testing/ packages/cubejs-testing/
# COPY packages/cubejs-docker/ packages/cubejs-docker/
# Frontend
#COPY packages/cubejs-templates/ packages/cubejs-templates/
#COPY packages/cubejs-client-core/ packages/cubejs-client-core/
#COPY packages/cubejs-client-react/ packages/cubejs-client-react/
#COPY packages/cubejs-client-vue/ packages/cubejs-client-vue/
#COPY packages/cubejs-client-vue3/ packages/cubejs-client-vue3/
#COPY packages/cubejs-client-ngx/ packages/cubejs-client-ngx/
#COPY packages/cubejs-client-ws-transport/ packages/cubejs-client-ws-transport/
#COPY packages/cubejs-playground/ packages/cubejs-playground/

RUN yarn install
RUN yarn lerna run build

RUN find . -name 'node_modules' -type d -prune -exec rm -rf '{}' +

FROM node:18.20.3-bullseye-slim AS final

ARG IMAGE_VERSION=dev

ENV CUBEJS_DOCKER_IMAGE_VERSION=$IMAGE_VERSION
ENV CUBEJS_DOCKER_IMAGE_TAG=latest

RUN DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get install -y --no-install-recommends rxvt-unicode libssl1.1 python3 libpython3-dev \
    && rm -rf /var/lib/apt/lists/*

ENV TERM rxvt-unicode
ENV NODE_ENV production

WORKDIR /cube
COPY packages/cubejs-docker/bin bin
# Unlike latest.Dockerfile, this one doesn't install the latest cubejs from
# npm, but rather copies all the artifacts from the dev image and links them to
# the /cube directory
COPY --from=build /cubejs /cube-build
RUN cd /cube-build && yarn run link:transai
COPY packages/cubejs-docker/package.json.transai package.json

RUN yarn policies set-version v1.22.19
# Yarn v1 uses aggressive timeouts with summing time spending on fs, https://github.com/yarnpkg/yarn/issues/4890
RUN yarn config set network-timeout 120000 -g

# We are copying root yarn.lock file to the context folder during the Publish GH
# action. So, a process will use the root lock file here.
RUN yarn install --prod && yarn cache clean && yarn link:dev

# By default Node dont search in parent directory from /cube/conf, @todo Reaserch a little bit more
ENV NODE_PATH /cube/conf/node_modules:/cube/node_modules
ENV PYTHONUNBUFFERED=1
RUN ln -s /cube/node_modules/.bin/cubejs /usr/local/bin/cubejs
RUN ln -s /cube/node_modules/.bin/cubestore-dev /usr/local/bin/cubestore-dev

WORKDIR /cube/conf

EXPOSE 4000

CMD ["cubejs", "server"]

