FROM node:12.18-alpine

VOLUME /configuration

WORKDIR /www
COPY . .

RUN yarn install
RUN npm run tsc && ln -s /www/bin/cube /usr/local/bin/cube

EXPOSE 4000

ENTRYPOINT ["cube", "server"]
