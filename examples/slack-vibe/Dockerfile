FROM node:lts
COPY frontend /app/frontend
COPY backend /app/backend
WORKDIR /app/frontend
RUN npm ci --only=production
RUN npm run build
WORKDIR /app/backend
RUN npm ci --only=production
EXPOSE 4000
CMD [ "npm", "start" ]
