FROM node:16.20.1-bullseye-slim AS base

WORKDIR /cube

COPY package.json .
COPY lerna.json .
COPY yarn.lock .
COPY tsconfig.base.json .

ENV CUBESTORE_SKIP_POST_INSTALL=true

COPY packages/cubejs-backend-shared/package.json packages/cubejs-backend-shared/package.json
COPY packages/cubejs-base-driver/package.json packages/cubejs-base-driver/package.json
COPY packages/cubejs-backend-native/package.json packages/cubejs-backend-native/package.json
COPY packages/cubejs-api-gateway/package.json packages/cubejs-api-gateway/package.json
COPY packages/cubejs-cli/package.json packages/cubejs-cli/package.json
COPY packages/cubejs-cubestore-driver/package.json packages/cubejs-cubestore-driver/package.json
COPY packages/cubejs-prestodb-driver/package.json packages/cubejs-prestodb-driver/package.json
COPY packages/cubejs-trino-driver/package.json packages/cubejs-trino-driver/package.json
COPY packages/cubejs-query-orchestrator/package.json packages/cubejs-query-orchestrator/package.json
COPY packages/cubejs-schema-compiler/package.json packages/cubejs-schema-compiler/package.json
COPY packages/cubejs-server/package.json packages/cubejs-server/package.json
COPY packages/cubejs-server-core/package.json packages/cubejs-server-core/package.json

FROM base AS prod_deps

# Use yarn v2 because of https://github.com/yarnpkg/yarn/issues/6323
RUN yarn set version berry
RUN yarn plugin import workspace-tools
RUN yarn config set nodeLinker node-modules
RUN yarn workspaces focus --production --all

FROM base AS builder

COPY packages/cubejs-backend-shared/ packages/cubejs-backend-shared/
COPY packages/cubejs-base-driver/ packages/cubejs-base-driver/
COPY packages/cubejs-backend-native/ packages/cubejs-backend-native/
COPY packages/cubejs-api-gateway/ packages/cubejs-api-gateway/
COPY packages/cubejs-cli/ packages/cubejs-cli/
COPY packages/cubejs-cubestore-driver/ packages/cubejs-cubestore-driver/
COPY packages/cubejs-prestodb-driver/ packages/cubejs-prestodb-driver/
COPY packages/cubejs-trino-driver/ packages/cubejs-trino-driver/
COPY packages/cubejs-query-orchestrator/ packages/cubejs-query-orchestrator/
COPY packages/cubejs-schema-compiler/ packages/cubejs-schema-compiler/
COPY packages/cubejs-server/ packages/cubejs-server/
COPY packages/cubejs-server-core/ packages/cubejs-server-core/

RUN yarn install
RUN yarn lerna run build

RUN find . -name 'node_modules' -type d -prune -exec rm -rf '{}' +

FROM base AS final
ARG IMAGE_VERSION=unknown

COPY --from=builder /cube .
COPY --from=prod_deps /cube .

ENV CUBEJS_DOCKER_IMAGE_TAG=latest
ENV CUBEJS_DOCKER_IMAGE_VERSION=$IMAGE_VERSION

ENV NODE_ENV production
ENV NODE_PATH /cube/conf/node_modules:/cube/node_modules
# I'm not sure why yarn is not automatically creating this bin file, but we just do it manually here
RUN chmod +x /cube/packages/cubejs-cli/dist/src/index.js && ln -s /cube/packages/cubejs-cli/dist/src/index.js  /usr/local/bin/cubejs

WORKDIR /cube/conf

EXPOSE 4000

CMD ["cubejs", "server"]
