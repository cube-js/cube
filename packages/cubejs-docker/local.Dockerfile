ARG DEV_BUILD_IMAGE=cubejs/cube:build

FROM $DEV_BUILD_IMAGE as build
FROM node:16.20.0-bullseye-slim

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
COPY . .
# Unlike latest.Dockerfile, this one doesn't install the latest cubejs from
# npm, but rather copies all the artifacts from the dev image and links them to
# the /cube directory
COPY --from=build /cubejs /cube-build
RUN cd /cube-build && yarn run link:dev
COPY package.json.local package.json

RUN yarn policies set-version v1.22.19
# Yarn v1 uses aggressive timeouts with summing time spending on fs, https://github.com/yarnpkg/yarn/issues/4890
RUN yarn config set network-timeout 120000 -g

# Required for node-oracledb to buld on ARM64
RUN apt-get update \
    && apt-get install -y python3 gcc g++ make cmake \
    && rm -rf /var/lib/apt/lists/*

# We are copying root yarn.lock file to the context folder during the Publish GH
# action. So, a process will use the root lock file here.
RUN yarn install --prod && yarn cache clean && yarn link:dev

# By default Node dont search in parent directory from /cube/conf, @todo Reaserch a little bit more
ENV NODE_PATH /cube/conf/node_modules:/cube/node_modules
RUN ln -s /cube/node_modules/.bin/cubejs /usr/local/bin/cubejs
RUN ln -s /cube/node_modules/.bin/cubestore-dev /usr/local/bin/cubestore-dev

WORKDIR /cube/conf

EXPOSE 4000

CMD ["cubejs", "server"]
