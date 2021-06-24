---
title: Deployment Guide
permalink: /deployment/guide
category: Deployment
menuOrder: 2
redirect_from:
  - /deployment/
---

This section contains guides, best practices and advices related to deploying
and managing Cube.js in production.

If you are moving Cube.js to production, check this guide:

[Production Checklist](/deployment/production-checklist)

&nbsp;

Below you can find guides for popular deployment environments:

- [Docker](/deployment/platforms/docker)
- [AWS Serverless](#aws-serverless)
- [GCP Serverless](#gcp-serverless)
- [Heroku](#heroku)
- [Cube Cloud](#cube-cloud)

## AWS Serverless

Cube.js can also be deployed in serverless mode with
[Serverless Framework](https://serverless.com/). The following guide shows how
to setup deployment to AWS Lambda.

### Create New Cube.js Service with Serverless template

Create Cube.js Serverless app:

```bash
npm install -g serverless
npx cubejs-cli create cubejs-serverless -d athena -t serverless
```

### Setup Redis

Create AWS ElasticCache Redis instance within the same region as your Serverless
Framework deployment. Add the Redis security group and subnets to
`serverless.yml` VPC settings:

```yaml
provider:
  vpc:
    securityGroupIds:
      - sg-12345678901234567 # Your DB and Redis security groups here
    subnetIds:
      - subnet-12345678901234567 # Your DB and Redis subnets here
```

### Add internet access for Lambda

When you assign a VPC to a Lambda function, internet access will be disabled by
default. Lambda functions require internet access to send SNS messages for query
processing. Please follow [this guide][link-vpc-internet] to set up internet
access for your Lambda functions or use [this template][link-vpc-cf-template] to
create the VPC, subnets and NAT configuration using CloudFormation.

[link-vpc-internet]:
  https://medium.com/@philippholly/aws-lambda-enable-outgoing-internet-access-within-vpc-8dd250e11e12
[link-vpc-cf-template]:
  https://raw.githubusercontent.com/awsdocs/aws-lambda-developer-guide/main/templates/vpc-privatepublic.yaml

### Athena permissions

Please add following permissions to `serverless.yml` if you need to access AWS
Athena within your Lambda functions:

```yaml
provider:
  iamRoleStatements:
    - Effect: 'Allow'
      Action:
        - 'sns:*'
        # Athena permissions
        - 'athena:*'
        - 's3:*'
        - 'glue:*'
      Resource:
        - '*'
```

### Deploy

```bash
serverless deploy -v
```

### Logs

```bash
serverless logs -t -f cubejs
serverless logs -t -f cubejsProcess
```

### Passing server core options for serverless

[Server options][ref-config] can be passed by instantiating the appropriate
`Handlers` class directly, for example:

[ref-config]: /config

**index.js:**

```javascript
const AWSHandlers = require('@cubejs-backend/serverless-aws');
const MySQLDriver = require('@cubejs-backend/mysql-driver');

module.exports = new AWSHandlers({
  externalDbType: 'mysql',
  externalDriverFactory: () =>
    new MySQLDriver({
      host: process.env.CUBEJS_EXT_DB_HOST,
      database: process.env.CUBEJS_EXT_DB_NAME,
      port: process.env.CUBEJS_EXT_DB_PORT,
      user: process.env.CUBEJS_EXT_DB_USER,
      password: process.env.CUBEJS_EXT_DB_PASS,
    }),
});
```

## GCP Serverless

Cube.js can also be deployed to Google Cloud Platform in serverless mode.

```bash
npm install -g serverless
npx cubejs-cli create -d bigquery -t serverless-google
```

Update all placeholders in just created `serverless.yml`.

### Setup Redis

1. [Create Redis](https://cloud.google.com/memorystore/docs/redis/quickstart-console).
2. [Setup Serverless VPC Access Connector](https://cloud.google.com/functions/docs/connecting-vpc).

### Deploy

```bash
serverless deploy -v
```

As the [`serverless-google-cloudfunctions`][link-serverless-gcp-plugin] plugin
doesn't support vpc-connector, you'll need to [set it
manually][link-gcp-set-vpc] each time after deploy.

[link-serverless-gcp-plugin]:
  https://github.com/serverless/serverless-google-cloudfunctions
[link-gcp-set-vpc]:
  https://cloud.google.com/functions/docs/connecting-vpc#configuring

### Logs

```bash
serverless logs -t -f cubejs
serverless logs -t -f cubejsProcess
```

<!-- prettier-ignore-start -->
[[warning | Warning]]
| It is suitable to host single Node applications this way without any
| significant load anticipated. Please consider deploying Cube.js as a
| microservice inside Docker if you need to host multiple Cube.js instances.
<!-- prettier-ignore-end -->

## Heroku

Heroku Container Registry allows you to deploy your Docker images to Heroku.
Both Common Runtime and Private Spaces are supported.

### Create new Cube.js app

```bash
npx cubejs-cli create cubejs-heroku-demo -d postgres
cd cubejs-heroku-demo
```

### Create new Heroku app

```bash
$ heroku create cubejs-heroku-demo

Creating ⬢ cubejs-heroku-demo... done
https://cubejs-heroku-demo.herokuapp.com/ | https://git.heroku.com/cubejs-heroku-demo.git
```

### Init a Docker image

```bash
touch Dockerfile
touch .dockerignore
```

Example `Dockerfile`:

```dockerfile
FROM cubejs/cube:latest

COPY . .
```

Example `.dockerignore`:

```bash
node_modules
npm-debug.log
.env
```

### Build the Docker image

Log in to the Container Registry:

```bash
heroku container:login
```

Build the image and push it to the Container Registry:

```bash
heroku container:push web -a cubejs-heroku-demo
```

Then release the image to your app:

```bash
heroku container:release web -a cubejs-heroku-demo
```

### Set up database connection

```bash
heroku config:set -a cubejs-heroku-demo \
  CUBEJS_DB_TYPE=<YOUR-DB-TYPE> \
  CUBEJS_DB_HOST=<YOUR-DB-HOST> \
  CUBEJS_DB_NAME=<YOUR-DB-NAME> \
  CUBEJS_DB_USER=<YOUR-DB-USER> \
  CUBEJS_DB_PASS=<YOUR-DB-PASSWORD> \
  CUBEJS_API_SECRET=<RANDOM_B64_STRING_FROM_ENV_FILE>
```

### Provision Redis

You can use any Redis server. If you don't have one, you can use a free Redis
server provided by Heroku:

```bash
heroku addons:create heroku-redis:hobby-dev -a cubejs-heroku-demo
```

If you use another Redis server, you should pass your Redis URL as an
environment variable:

```bash
heroku config:set CUBEJS_REDIS_URL=<YOUR-REDIS-URL> -a cubejs-heroku-demo
```

Note that Cube.js requires at least 15 concurrent connections allowed by Redis
server. Please [setup connection pool](/deployment#production-mode-redis-pool)
according to your Redis server's maximum connections.

### Building Docker Images with Heroku

Create a `heroku.yml` file in your application’s root directory:

```yaml
build:
  docker:
    web: Dockerfile
```

Commit the file to your repo:

```sh
git add heroku.yml
git commit -m "Add heroku.yml"
```

Set the stack of your app to `container`:

```sh
heroku stack:set container
```

Push your app to Heroku:

```sh
git push heroku master
```

For more details, take a look at the
[official documentation from Heroku](https://devcenter.heroku.com/articles/build-docker-images-heroku-yml).

## Cube Cloud

<!-- prettier-ignore-start -->
[[info | ]]
| [Cube Cloud][link-cube-cloud] currently is in early access. If you don't have
| an account yet, you can [sign up to the waitlist here][link-cube-cloud].
<!-- prettier-ignore-end -->

[link-cube-cloud]: https://cube.dev/cloud

Cube Cloud is a purpose-built platform to run Cube.js applications in
production. It is made by the creators of Cube.js and incorporates all the best
practices of running and scaling Cube.js applications.

![](https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Deployment/how-cube-cloud-works.png)

Cube Cloud can be integrated with your GitHub to automatically deploy from the
specified production branch (`master` by default). It can also create staging
and preview APIs based on the branches in the repository.

You can learn more about [deployment with Cube Cloud](/cloud/deploys) in its
documentation.
