# syntax=docker/dockerfile-upstream:master-experimental
FROM node:24.22.1-trixie-slim AS builder

# Use the image's Node headers to avoid concurrent node-gyp downloads and copies.
ENV npm_config_nodedir=/usr/local

WORKDIR /cube

# Required for node-oracledb to buld on ARM64
RUN apt-get update \
    # libpython3-dev is needed to trigger post-installer to download native with python
    && apt-get install -y python3.13 libpython3.13-dev gcc g++ make cmake openjdk-21-jdk-headless \
    && update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.13 1 \
    && update-alternatives --install /usr/bin/python python /usr/bin/python3.13 1 \
    && rm -rf /var/lib/apt/lists/*

COPY . .

# The Publish GH action copies the root yarn.lock, .yarnrc.yml and Yarn release
# to the context folder, so the install is pinned by the root lock file.
RUN yarn workspaces focus --production \
    && yarn cache clean --all \
    # Users run their own Yarn in /cube/conf, so the monorepo's Yarn must not leak into the image
    && rm -rf .yarn .yarnrc.yml

FROM node:24.22.1-trixie-slim

ARG IMAGE_VERSION=unknown

ENV CUBEJS_DOCKER_IMAGE_VERSION=$IMAGE_VERSION
ENV CUBEJS_DOCKER_IMAGE_TAG=latest

RUN groupadd cube && useradd -ms /bin/bash -g cube cube \
    && DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get install -y --no-install-recommends libssl3t64 openjdk-21-jre-headless python3.13 libpython3.13-dev \
    && update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.13 1 \
    && update-alternatives --install /usr/bin/python python /usr/bin/python3.13 1 \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir cube \
    && chown -R cube:cube /tmp /cube /usr

USER cube
WORKDIR /cube

RUN yarn policies set-version v1.22.22

ENV NODE_ENV production

COPY --chown=cube:cube --from=builder /cube .

# By default Node dont search in parent directory from /cube/conf, @todo Reaserch a little bit more
ENV NODE_PATH /cube/conf/node_modules:/cube/node_modules
ENV PYTHONUNBUFFERED=1
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
RUN ln -s /cube/node_modules/.bin/cubejs /usr/local/bin/cubejs
RUN ln -s /cube/node_modules/.bin/cubestore-dev /usr/local/bin/cubestore-dev

WORKDIR /cube/conf

EXPOSE 4000

CMD ["cubejs", "server"]
