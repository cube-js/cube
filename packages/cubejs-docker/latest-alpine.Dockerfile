FROM node:12.20.1-alpine

ARG IMAGE_VERSION=unknown

ENV CUBEJS_DOCKER_IMAGE_VERSION=$IMAGE_VERSION
ENV CUBEJS_DOCKER_IMAGE_TAG=alpine

RUN apk add rxvt-unicode

# Cube Store dosnt ship musl build now, let's use build glibc in alpine (which is using musl)
# Probably in near feature we will finish musl builds, but for now I dont know the way. Do you know the way?
ENV PKG_GLIBC_VERSION 2.32-r0
ENV LANG=C.UTF-8

RUN apk add --update --no-cache libstdc++6 curl \
    && cd /tmp \
    && wget -q -O /etc/apk/keys/sgerrand.rsa.pub https://alpine-pkgs.sgerrand.com/sgerrand.rsa.pub \
    && curl -L https://github.com/sgerrand/alpine-pkg-glibc/releases/download/${PKG_GLIBC_VERSION}/glibc-${PKG_GLIBC_VERSION}.apk -o glibc.apk \
    && curl -L https://github.com/sgerrand/alpine-pkg-glibc/releases/download/${PKG_GLIBC_VERSION}/glibc-bin-${PKG_GLIBC_VERSION}.apk -o glibc-bin.apk \
    && curl -L https://github.com/sgerrand/alpine-pkg-glibc/releases/download/${PKG_GLIBC_VERSION}/glibc-i18n-${PKG_GLIBC_VERSION}.apk -o glibc-i18n.apk \
    && apk add glibc.apk glibc-bin.apk glibc-i18n.apk \
    && apk del curl \
    && rm glibc.apk glibc-bin.apk glibc-i18n.apk

# For now Cube.js docker image is building without waiting cross jobs, it's why we are not able to install it
ENV CUBESTORE_SKIP_POST_INSTALL=true
ENV TERM rxvt-unicode
ENV NODE_ENV production

WORKDIR /cube
COPY . .

# There is a problem with release process.
# We are doing version bump without updating lock files for the docker package.
#RUN yarn install --frozen-lockfile
RUN yarn install

# By default Node dont search in parent directory from /cube/conf, @todo Reaserch a little bit more
ENV NODE_PATH /cube/conf/node_modules:/cube/node_modules
RUN ln -s /cube/node_modules/.bin/cubejs /usr/local/bin/cubejs

WORKDIR /cube/conf

EXPOSE 4000

CMD ["cubejs", "server"]
