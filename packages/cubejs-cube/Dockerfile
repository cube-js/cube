FROM node:14.11-alpine

VOLUME /configuration

WORKDIR /www
COPY . .

RUN yarn install
RUN npm run tsc

EXPOSE 4000

ENTRYPOINT ["npm"]
