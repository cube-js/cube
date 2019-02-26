---
title: Deployment
permalink: /deployment
category: Cube.js Backend
---

## Serverless

Cube.js could be deployed in serverless mode with [Serverless
Framework](https://serverless.com/). The following guide shows how to setup
deployment to AWS Lambda.

### Create New Cube.js Service with Serverless template

Create Cube.js Serverless app:

```bash
$ npm install -g serverless
$ cubejs create -d athena -t serverless
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
