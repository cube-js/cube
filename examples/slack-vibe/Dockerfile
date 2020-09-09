FROM node:lts-slim AS frontend
WORKDIR /app
COPY ./frontend /app
RUN npm ci --only=production
RUN npm run build

FROM node:lts-slim AS backend
WORKDIR /app
COPY . /app
RUN rm -rf frontend && npm ci --only=production
COPY --from=frontend /app/build ./frontend/build
EXPOSE 4000
CMD [ "npm", "start" ]
