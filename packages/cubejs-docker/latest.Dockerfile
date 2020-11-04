FROM node:12.19

ENV CUBEJS_DOCKER_IMAGE_TAG=latest

WORKDIR /cube
COPY . .

RUN yarn install --frozen-lockfile

# By default Node dont search in parent directory from /cube/conf, @todo Reaserch a little bit more
ENV NODE_PATH /cube/conf/node_modules:/cube/node_modules
RUN ln -s /cube/node_modules/.bin/cubejs /usr/local/bin/cubejs

VOLUME /cube/conf
WORKDIR /cube/conf

EXPOSE 4000

ENTRYPOINT ["cubejs", "server"]
