FROM node:12.22.1

ARG IMAGE_VERSION=dev

ENV CUBEJS_DOCKER_IMAGE_VERSION=$IMAGE_VERSION
ENV CUBEJS_DOCKER_IMAGE_TAG=dev
ENV CI=0

RUN DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get install -y --no-install-recommends rxvt-unicode libssl1.1 \
    && rm -rf /var/lib/apt/lists/*

ENV CUBESTORE_SKIP_POST_INSTALL=true
ENV TERM rxvt-unicode
ENV NODE_ENV development

WORKDIR /cubejs

COPY package.json .
