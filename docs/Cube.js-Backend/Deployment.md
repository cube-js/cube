---
title: Deployment
permalink: /deployment
category: Cube.js Backend
menuOrder: 3
---

## Prerequisites

Running Cube.js Backend in production requres some changes in configuration:

  * Set `NODE_ENV` environment variable to `production`
  * Provide Redis using environment variable `REDIS_URL`

On Heroku, `NODE_ENV` is set to `production` by default.
Running Cube.js Backend in development doesn't require Redis.

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
```

Example .dockerignore

```
node_modules
npm-debug.log
.env
schema
```

### Build Docker image

```bash
$ docker build -t <YOUR-USERNAME>/cubejs-docker-demo .
```

### Run Docker image

To run docker image, you have to set environment variables needed for Cube.js Backend to work.
Generate a secret for JWT Tokens as described in [Security](/security) section and fill in database credentials.
Also you have to provide a path to the directory for [Data schema](/getting-started-cubejs-schema) files.

```bash
$ docker run -p 49160:8080 \
  -d \
  --name cubejs-docker-demo \
  -e CUBEJS_API_SECRET=<YOUR-API-SECRET> \
  -e CUBEJS_DB_HOST=<YOUR-DB-HOST-HERE> \
  -e CUBEJS_DB_NAME=<YOUR-DB-NAME-HERE> \
  -e CUBEJS_DB_USER=<YOUR-DB-USER-HERE> \
  -e CUBEJS_DB_PASS=<YOUR-DB-PASS-HERE> \
  -e CUBEJS_DB_TYPE=postgres \
  -v <PATH-TO-SCHEMA>:/usr/src/app/schema \
  <YOUR-USERNAME>/cubejs-docker-demo
```

### Stop Docker image

```bash
$ docker stop cubejs-docker-demo
```