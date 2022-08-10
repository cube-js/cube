# syntax=docker/dockerfile-upstream:master-experimental
FROM node:14.18.2-buster-slim

ARG IMAGE_VERSION=unknown

ENV CUBEJS_DOCKER_IMAGE_VERSION=$IMAGE_VERSION
ENV CUBEJS_DOCKER_IMAGE_TAG=latest

RUN DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get install -y --no-install-recommends rxvt-unicode libssl1.1 \
    && rm -rf /var/lib/apt/lists/*

ENV TERM rxvt-unicode
ENV NODE_ENV production

# Required for node-oracledb to buld on ARM64
RUN groupadd cube \
    && useradd -ms /bin/bash -g cube cube \
    && apt-get clean  \
    && apt-get update \
    && apt-get install -y python2 python3 gcc g++ make cmake openjdk-11-jdk-headless \
    && npm config set python /usr/bin/python2.7 \
    && rm -rf /var/lib/apt/lists/* \
    && chown -R cube:cube /tmp /home/cube /usr

USER cube
WORKDIR /home/cube
COPY --chown=cube:cube . .

# There is a problem with release process.
# We are doing version bump without updating lock files for the docker package.
#RUN yarn install --frozen-lockfile
ENV CUBEJS_DB_DATABRICKS_ACCEPT_POLICY=true
ARG CUBEJS_JDBC_DRIVER_TAG="0.30.20"
RUN yarn policies set-version v1.22.5 \
    && yarn add "@cubejs-backend/jdbc-driver@<=$CUBEJS_JDBC_DRIVER_TAG" "@cubejs-backend/databricks-jdbc-driver@<=$CUBEJS_JDBC_DRIVER_TAG" \
    && yarn install && yarn cache clean

# By default Node dont search in parent directory from /cube/conf, @todo Reaserch a little bit more
ENV NODE_PATH /home/cube/conf/node_modules:/home/cube/node_modules
RUN ln -s /home/cube/node_modules/.bin/cubejs /usr/local/bin/cubejs \
    && ln -s /home/cube/node_modules/.bin/cubestore-dev /usr/local/bin/cubestore-dev

WORKDIR /home/cube/conf

EXPOSE 4000

CMD ["cubejs", "server"]
