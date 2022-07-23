FROM node:14.18.2-alpine3.14

ARG IMAGE_VERSION=unknown

ENV CUBEJS_DOCKER_IMAGE_VERSION=$IMAGE_VERSION
ENV CUBEJS_DOCKER_IMAGE_TAG=alpine

RUN apk add rxvt-unicode

ENV TERM rxvt-unicode
ENV NODE_ENV production

WORKDIR /cube
COPY . .

RUN yarn policies set-version v1.22.5

# Required for node-oracledb to buld on ARM64
RUN apk update \
    && apk add python2 gcc g++ make cmake bash openjdk11-jdk \
    && npm config set python /usr/bin/python2.7 \
    && rm -rf /var/cache/apk/*

ENV JAVA_HOME /usr/lib/jvm/default-jvm
ENV PATH "$JAVA_HOME/bin:${PATH}"

# There is a problem with release process.
# We are doing version bump without updating lock files for the docker package.
#RUN yarn install --frozen-lockfile
RUN yarn install && yarn cache clean

# By default Node dont search in parent directory from /cube/conf, @todo Reaserch a little bit more
ENV NODE_PATH /cube/conf/node_modules:/cube/node_modules
RUN ln -s /cube/node_modules/.bin/cubejs /usr/local/bin/cubejs
RUN ln -s /cube/node_modules/.bin/cubestore-dev /usr/local/bin/cubestore-dev

WORKDIR /cube/conf

EXPOSE 4000

CMD ["cubejs", "server"]
