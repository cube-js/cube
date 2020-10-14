FROM node:12.19-alpine

ENV CUBEJS_DOCKER_IMAGE_TAG=latest

WORKDIR /cube
COPY . .

RUN yarn install
COPY bin/cubejs /usr/local/bin/cubejs

VOLUME /cube/conf
WORKDIR /cube/conf

EXPOSE 4000

ENTRYPOINT ["cubejs", "server"]
