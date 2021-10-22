FROM node:12.22.6

ARG IMAGE_VERSION=unknown

ENV CUBEJS_DOCKER_IMAGE_VERSION=$IMAGE_VERSION
ENV CUBEJS_DOCKER_IMAGE_TAG=latest

RUN DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get install -y --no-install-recommends rxvt-unicode libssl1.1 \
    && rm -rf /var/lib/apt/lists/*

ENV TERM rxvt-unicode
ENV NODE_ENV production

WORKDIR /cube
COPY . .

RUN yarn policies set-version v1.22.5

# There is a problem with release process.
# We are doing version bump without updating lock files for the docker package.
#RUN yarn install --frozen-lockfile
RUN yarn install

# By default Node dont search in parent directory from /cube/conf, @todo Reaserch a little bit more
ENV NODE_PATH /cube/conf/node_modules:/cube/node_modules
RUN ln -s /cube/node_modules/.bin/cubejs /usr/local/bin/cubejs
RUN ln -s /cube/node_modules/.bin/cubestore-dev /usr/local/bin/cubestore-dev

WORKDIR /cube/conf

EXPOSE 4000

CMD ["cubejs", "server"]
