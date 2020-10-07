FROM node:12.18-alpine

WORKDIR /www
COPY . .

RUN yarn install
RUN npm run tsc && ln -s /www/bin/cube /usr/local/bin/cube

VOLUME /cube/conf
WORKDIR /cube/conf

EXPOSE 4000

ENTRYPOINT ["cube", "server"]
