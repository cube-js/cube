FROM node:lts-slim
COPY . /app
WORKDIR /app/frontend
RUN npm ci --only=production
RUN npm run build
WORKDIR /app
RUN npm ci --only=production
EXPOSE 4000
CMD [ "npm", "start" ]
