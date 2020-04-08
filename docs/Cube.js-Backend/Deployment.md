---
title: Deployment
permalink: /deployment
category: Cube.js Backend
menuOrder: 3
---

Below you can find guides for popular deployment environments:

- [As a part of Express application](#express)
- [AWS Lambda with Serverless Framework](#serverless)
- [Heroku](#heroku)
- [Docker](#docker)
- [Docker Compose](#docker-compose)

## Production Mode

When running Cube.js Backend in production make sure `NODE_ENV` is set to `production`. 
Such platforms, such as Heroku, do it by default.
In this mode Cube.js unsecured development server and Playground will be disabled by default because there's a security risk serving those in production environments.
Production Cube.js servers can be accessed only with [REST API](rest-api) and Cube.js frontend libraries. 

### Redis

Also, Cube.js requires [Redis](https://redis.io/), in-memory data structure store, to run in production. 
It uses Redis for query caching and queue. 
Set `REDIS_URL` environment variable to provide Cube.js with Redis connection. In case your Redis instance has password, please set password via `REDIS_PASSWORD` environment variable. 
Make sure, your Redis allows at least 15 concurrent connections.
Set `REDIS_TLS` env variable to `true` if you want to enable secure connection.

### Redis Pool

If `REDIS_URL` is provided Cube.js will create Redis pool with 2 min and 1000 max of concurrent connections by default.
`CUBEJS_REDIS_POOL_MIN` and `CUBEJS_REDIS_POOL_MAX` environment variables can be used to tweak pool size.
No pool behavior with each connection created on demand can be achieved with `CUBEJS_REDIS_POOL_MAX=0` setting.

### Running without Redis

If you want to run Cube.js in production without redis you can use `CUBEJS_CACHE_AND_QUEUE_DRIVER=memory` env setting.

> **NOTE:** Serverless and clustered deployments can't be run without Redis as it's used to manage querying queue.

## Express

Cube.js server is an Express application itself and it can be served as part of an existing Express application.
Minimal setup for such serving looks as following:

**index.js**
```javascript
require('dotenv').config();
const express = require('express');
const bodyParser = require('body-parser');
const CubejsServerCore = require('@cubejs-backend/server-core');

const app = express();
app.use(require('cors')());
app.use(bodyParser.json({ limit: '50mb' }));

const serverCore = CubejsServerCore.create();
serverCore.initApp(app);

const port = process.env.PORT || 4000;
app.listen(port, (err) => {
  if (err) {
    console.error('Fatal error during server start: ');
    console.error(e.stack || e);
  }
  console.log(`🚀 Cube.js server is listening on ${port}`);
});
```

## Express with Basic Passport Authentication

To serve simple dashboard application with minimal basic authentication security following setup can be used:

**index.js**
```javascript
require('dotenv').config();
const http = require('http');
const express = require('express');
const bodyParser = require('body-parser');
const cookieParser = require('cookie-parser');
const session = require('express-session');
const passport = require('passport');
const serveStatic = require('serve-static');
const path = require('path');
const { BasicStrategy } = require('passport-http');
const CubejsServerCore = require('@cubejs-backend/server-core');

const app = express();
app.use(require('cors')());
app.use(cookieParser());
app.use(bodyParser.json({ limit: '50mb' }));
app.use(session({ secret: process.env.CUBEJS_API_SECRET }));
app.use(passport.initialize());
app.use(passport.session());

passport.use(new BasicStrategy(
  (user, password, done) => {
    if (user === 'admin' && password === 'admin') {
      done(null, { user });
    } else {
      done(null, false);
    }
  }
));

passport.serializeUser((user, done) => done(null, user));
passport.deserializeUser((user, done) => done(null, user));

app.get('/login', passport.authenticate('basic'), (req, res) => {
  res.redirect('/')
});

app.use((req, res, next) => {
  if (!req.user) {
    res.redirect('/login');
    return;
  }
  next();
});

if (process.env.NODE_ENV === 'production') {
  app.use(serveStatic(path.join(__dirname, 'dashboard-app/build')));
}

const serverCore = CubejsServerCore.create({
  checkAuth: (req, auth) => {
    if (!req.user) {
      throw new Error(`Unauthorized`);
    }
    req.authInfo = { u: req.user };
  }
});

serverCore.initApp(app);

const port = process.env.PORT || 4000;
const server = http.createServer(app);

server.listen(port, (err) => {
  if (err) {
    console.error('Fatal error during server start: ');
    console.error(e.stack || e);
  }
  console.log(`🚀 Cube.js server is listening on ${port}`);
});
```

Use **admin / admin** as **user / password** to access the dashboard.

## Serverless

Cube.js could be deployed in serverless mode with [Serverless
Framework](https://serverless.com/). The following guide shows how to setup
deployment to AWS Lambda.

### Create New Cube.js Service with Serverless template

Create Cube.js Serverless app:

```bash
$ npm install -g serverless
$ cubejs create cubejs-serverless -d athena -t serverless
```

### Setup Redis

Create AWS ElasticCache Redis instance within the same region where lambda hosts.

Add Redis security group and subnet to `serverless.yml` vpc settings:

```yaml
provider:
  vpc:
    securityGroupIds:
     - sg-12345678901234567 # Your DB and Redis security groups here
    subnetIds:
     - subnet-12345678901234567 # Your DB and Redis subnets here
```

### Add internet access for Lambda

When you assign vpc to a Lambda functions internet access will be disabled by default.
Lambda functions require internet access to send SNS messages for query processing.
Please follow [this guide](https://medium.com/@philippholly/aws-lambda-enable-outgoing-internet-access-within-vpc-8dd250e11e12) to set up internet access for your Lambda functions.

### Athena permissions

Please add following permissions to `serverless.yml` if you need Athena within your Lambda functions:

```yaml
provider:
  iamRoleStatements:
    - Effect: "Allow"
      Action:
        - "sns:*"
# Athena permissions
        - "athena:*"
        - "s3:*"
        - "glue:*"
      Resource:
        - "*"
```

### Deploy

```bash
$ serverless deploy -v
```

### Logs

```bash
$ serverless logs -t -f cubejs
$ serverless logs -t -f cubejsProcess
```

### Passing server core options for serverless

[Server core options](@cubejs-backend-server-core#options-reference) can be passed by instantiating appropriate `Handlers` class directly.
For example:

**cube.js:**

```javascript
const AWSHandlers = require('@cubejs-backend/serverless-aws');
const MySQLDriver = require('@cubejs-backend/mysql-driver');

module.exports = new AWSHandlers({
  externalDbType: 'mysql',
  externalDriverFactory: () => new MySQLDriver({
    host: process.env.CUBEJS_EXT_DB_HOST,
    database: process.env.CUBEJS_EXT_DB_NAME,
    port: process.env.CUBEJS_EXT_DB_PORT,
    user: process.env.CUBEJS_EXT_DB_USER,
    password: process.env.CUBEJS_EXT_DB_PASS,
  })
});
```

## Heroku

### Create new app using Cube.js-CLI

```bash
$ cubejs create cubejs-heroku-demo -d postgres
$ cd cubejs-heroku-demo
```

### Init a git repository

```bash
$ git init
```

### Create new Heroku app

```bash
$ heroku create cubejs-heroku-demo
```

### Provision Redis

You can use any Redis server. If you don't have one, you can use a free Redis provided by Heroku:

```bash
$ heroku addons:create heroku-redis:hobby-dev -a cubejs-heroku-demo
```

If you use another Redis server, you should pass your Redis URL as an environment variable:

```bash
$ heroku config:set REDIS_URL:<YOUR-REDIS-URL>
```

Note that Cube.js requires at least 15 concurrent connections allowed by Redis server.

### Create Heroku Procfile

```bash
$ echo "web: node index.js" > Procfile
```

### Set up connection to your database

```bash
$ heroku config:set \
  CUBEJS_DB_HOST=<YOUR-DB-HOST> \
  CUBEJS_DB_NAME=<YOUR-DB-NAME> \
  CUBEJS_DB_USER=<YOUR-DB-USER> \
  CUBEJS_DB_PASS=<YOUR-DB-PASSWORD>
```

### Deploy app to Heroku

```bash
$ git add -A
$ git commit -am "Initial"
$ git push heroku master
```

## Docker

### Create new app using Cube.js-CLI

```bash
$ cubejs create cubejs-docker-demo -d postgres
$ cd cubejs-docker-demo
```

### Create Dockerfile and .dockerignore files

```bash
$ touch Dockerfile
$ touch .dockerignore
```

Example Dockerfile

```dockerfile
FROM node:10-alpine

WORKDIR /usr/src/app

COPY package*.json ./
RUN npm install
COPY . .
EXPOSE 4000

CMD ["index.js"]
```

Example .dockerignore

```bash
node_modules
npm-debug.log
.env
```

### Build Docker image

```bash
$ docker build -t <YOUR-USERNAME>/cubejs-docker-demo .
```

### Run Docker image

To test just built docker image, you need to set environment variables required by Cube.js Backend.
Generate a secret for JWT Tokens as described in [Security](/security) section and fill in database credentials.

```bash
$ docker --rm \
  --name cubejs-docker-demo \
  -e CUBEJS_API_SECRET=<YOUR-API-SECRET> \
  -e CUBEJS_DB_HOST=<YOUR-DB-HOST-HERE> \
  -e CUBEJS_DB_NAME=<YOUR-DB-NAME-HERE> \
  -e CUBEJS_DB_USER=<YOUR-DB-USER-HERE> \
  -e CUBEJS_DB_PASS=<YOUR-DB-PASS-HERE> \
  -e CUBEJS_DB_TYPE=postgres \
  <YOUR-USERNAME>/cubejs-docker-demo
```

### Stop Docker image

```bash
$ docker stop cubejs-docker-demo
```

## Docker Compose

To run the server in docker-compose we need to add a redis server and a .env file to include the environment variables needed to connect to the database, the secret api secret and redis hostname. 

Example .env file

```bash
REDIS_URL=redis://redis_db:6379/0

CUBEJS_DB_HOST=<YOUR-DB-HOST-HERE>
CUBEJS_DB_NAME=<YOUR-DB-NAME-HERE>
CUBEJS_DB_USER=<YOUR-DB-USER-HERE>
CUBEJS_DB_PASS=<YOUR-DB-PASS-HERE>
CUBEJS_DB_TYPE=postgres
CUBEJS_API_SECRET=<YOUR-API-SECRET>
```

Example docker-compose file

```yaml
redis_db:
  image: redis
  ports:
    - "6379"

cube:
  build: .
  env_file: .env
  expose:
    - "4000"
  command: node index.js
  links:
    - redis_db
```

`docker-compose` file should be in your Cube.js app main folder which contains `Dockerfile` and the `.env` file should be at the same level of the `docker-compose` file.

Build the containers

```bash
$ docker-compose build
```

### Start/Stop the containers 

```bash
$ docker-compose up
```

```bash
$ docker-compose stop
```


