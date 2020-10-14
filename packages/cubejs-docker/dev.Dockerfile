FROM node:12.19-alpine

ENV CUBEJS_DOCKER_IMAGE_TAG=dev

WORKDIR /cube

COPY package.json .
COPY lerna.json .
COPY yarn.lock .
COPY packages/cubejs-linter packages/cubejs-linter
RUN yarn install

# @todo https://stackoverflow.com/questions/49939960/docker-copy-files-using-glob-pattern/50010093
#COPY packages/*/package.json ./
#COPY packages/*/yarn.lock ./


COPY packages/cubejs-cli/package.json packages/cubejs-cli/package.json
COPY packages/cubejs-cli/yarn.lock packages/cubejs-cli/yarn.lock

#  --ignore @cubejs-backend/jdbc-driver not needed, because it's ignored by .dockerignore
RUN yarn lerna bootstrap

COPY packages .
RUN yarn lerna run build
RUN ln -s /cube/bin/cubejs /usr/local/bin/cubejs

VOLUME /cube/conf
WORKDIR /cube/conf

EXPOSE 4000

ENTRYPOINT ["cubejs", "server"]
