# syntax=docker/dockerfile-upstream:master-experimental
FROM node:22-trixie-slim AS builder

WORKDIR /cube
COPY . .

RUN yarn policies set-version v1.22.22
# Yarn v1 uses aggressive timeouts with summing time spending on fs, https://github.com/yarnpkg/yarn/issues/4890
RUN yarn config set network-timeout 120000 -g

# Required for node-oracledb to buld on ARM64
RUN apt-get update \
    # python3 package is necessary to install `python3` executable for node-gyp
    # libpython3-dev is needed to trigger post-installer to download native with python
    && apt-get install -y python3 python3-dev gcc g++ make cmake openjdk-21-jdk-headless wget \
    && rm -rf /var/lib/apt/lists/*

# We are copying root yarn.lock file to the context folder during the Publish GH
# action. So, a process will use the root lock file here.
RUN yarn install --prod \
    # Remove DuckDB sources to reduce image size
    && rm -rf /cube/node_modules/duckdb/src \
    && yarn cache clean \
    # FIX CVE-2019-10744: Patch lodash in unmaintained jshs2 package
    # jshs2 hasn't been updated since 2017 and bundles lodash 3.10.1 with critical vulnerabilities
    # This is a temporary fix until migration to hive-driver is completed
    && if [ -d /cube/node_modules/jshs2/node_modules/lodash ]; then \
        echo "Patching lodash in jshs2 from 3.10.1 to 4.17.21 (CVE-2019-10744 fix)" && \
        rm -rf /cube/node_modules/jshs2/node_modules/lodash && \
        cp -r /cube/node_modules/lodash /cube/node_modules/jshs2/node_modules/; \
    fi

# FIX CVE-2022-41853: Update hsqldb from 2.3.2 to 2.7.1
# Note: This is a JAR file that cannot be fixed via npm/yarn resolutions
RUN wget -O /tmp/hsqldb-2.7.1.jar https://repo1.maven.org/maven2/org/hsqldb/hsqldb/2.7.1/hsqldb-2.7.1.jar \
    && if [ -f /cube/node_modules/@cubejs-backend/jdbc/drivers-10.17/hsqldb.jar ]; then \
        mv /tmp/hsqldb-2.7.1.jar /cube/node_modules/@cubejs-backend/jdbc/drivers-10.17/hsqldb.jar; \
    fi \
    && rm -f /tmp/hsqldb-2.7.1.jar

FROM node:22-trixie-slim

ARG IMAGE_VERSION=unknown

ENV CUBEJS_DOCKER_IMAGE_VERSION=$IMAGE_VERSION
ENV CUBEJS_DOCKER_IMAGE_TAG=latest

RUN groupadd cube && useradd -ms /bin/bash -g cube cube \
    && DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get install -y --no-install-recommends libssl3 openjdk-21-jre-headless python3 python3-dev \
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
RUN ln -s /cube/node_modules/.bin/cubejs /usr/local/bin/cubejs
RUN ln -s /cube/node_modules/.bin/cubestore-dev /usr/local/bin/cubestore-dev

WORKDIR /cube/conf

EXPOSE 4000

CMD ["cubejs", "server"]
