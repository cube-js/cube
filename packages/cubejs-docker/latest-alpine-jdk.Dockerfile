FROM node:16.20.0-alpine3.17 as builder

WORKDIR /cube
COPY . .

RUN yarn policies set-version v1.22.19
# Yarn v1 uses aggressive timeouts with summing time spending on fs, https://github.com/yarnpkg/yarn/issues/4890
RUN yarn config set network-timeout 120000 -g

# Required for node-oracledb to buld on ARM64
RUN apk update \
    && apk add python3 gcc g++ make openjdk11-jdk \
    && rm -rf /var/cache/apk/*

ENV JAVA_HOME /usr/lib/jvm/default-jvm
ENV PATH "$JAVA_HOME/bin:${PATH}"

# We are copying root yarn.lock file to the context folder during the Publish GH
# action. So, a process will use the root lock file here.
RUN yarn install --prod && yarn cache clean

FROM node:16.20.0-alpine3.17

ARG IMAGE_VERSION=unknown

ENV CUBEJS_DOCKER_IMAGE_VERSION=$IMAGE_VERSION
ENV CUBEJS_DOCKER_IMAGE_TAG=alpine

RUN apk update \
    && apk add openjdk11-jdk rxvt-unicode \
    && rm -rf /var/cache/apk/*

ENV JAVA_HOME /usr/lib/jvm/default-jvm
ENV PATH "$JAVA_HOME/bin:${PATH}"

ENV TERM rxvt-unicode
ENV NODE_ENV production

WORKDIR /cube
COPY . .

RUN yarn policies set-version v1.22.19

WORKDIR /cube

COPY --from=builder /cube .

# By default Node dont search in parent directory from /cube/conf, @todo Reaserch a little bit more
ENV NODE_PATH /cube/conf/node_modules:/cube/node_modules
RUN ln -s /cube/node_modules/.bin/cubejs /usr/local/bin/cubejs
RUN ln -s /cube/node_modules/.bin/cubestore-dev /usr/local/bin/cubestore-dev

WORKDIR /cube/conf

EXPOSE 4000

CMD ["cubejs", "server"]
