---
title: Serverless Framework on AWS
permalink: /deployment/platforms/serverless/aws
category: Deployment
subCategory: Platforms
menuOrder: 2
---

This guide walks you through deploying Cube.js with the [Serverless
Framework][link-sls] on [AWS][link-aws].

<!-- prettier-ignore-start -->
[[warning |]]
| This is an example of a production-ready deployment, but real-world
| deployments require extra configuration to enable scheduled refreshes. Cube
| Store **cannot** be deployed using Serverless platforms. For a "serverless"
| experience, we highly recommend using [Cube Cloud][link-cube-cloud].
<!-- prettier-ignore-end -->

## Prerequisites

- An AWS account
- An [Elasticache Redis][aws-redis] cluster URL for caching and queuing
- A separate Cube Store deployment for pre-aggregations
- Node.js 12+
- Serverless Framework

## Configuration

Create a Serverless Framework project by creating a `serverless.yml`. A
production-ready stack would at minimum consist of:

- A [Lambda function][aws-lambda] for a Cube.js API instance
- A [Lambda function][aws-lambda] for a Cube.js Refresh Worker

The `serverless.yml` for an example project is provided below:

```yaml
service: hello-cube-sls

provider:
  name: aws
  runtime: nodejs12.x
  iamRoleStatements:
    - Effect: 'Allow'
      Action:
        - 'sns:*'
      Resource: '*'
  environment:
    CUBEJS_DB_TYPE: <YOUR_DB_TYPE_HERE>
    CUBEJS_DB_HOST: <YOUR_DB_HOST_HERE>
    CUBEJS_DB_NAME: <YOUR_DB_NAME_HERE>
    CUBEJS_DB_USER: <YOUR_DB_USER_HERE>
    CUBEJS_DB_PASS: <YOUR_DB_PASS_HERE>
    CUBEJS_DB_PORT: <YOUR_DB_PORT_HERE>
    CUBEJS_REDIS_URL: <YOUR_REDIS_URL_HERE>
    CUBEJS_API_SECRET: <YOUR_API_SECRET_HERE>
    CUBEJS_APP: '${self:service.name}-${self:provider.stage}'
    NODE_ENV: production
    AWS_ACCOUNT_ID:
      Fn::Join:
        - ''
        - - Ref: 'AWS::AccountId'

functions:
  cubejs:
    handler: index.api
    timeout: 30
    events:
      - http:
          path: /
          method: GET
      - http:
          path: /{proxy+}
          method: ANY
          cors:
            origin: '*'
            headers:
              - Content-Type
              - Authorization
              - X-Request-Id
              - X-Amz-Date
              - X-Amz-Security-Token
              - X-Api-Key
  cubejsProcess:
    handler: index.process
    timeout: 630
    events:
      - sns: '${self:service.name}-${self:provider.stage}-process'

plugins:
  - serverless-express
```

### Refresh Worker

<!-- prettier-ignore-start -->
[[warning |]]
| Running a refresh worker using Serverless requires a _slightly_ different
| setup than Docker. You must continuously call the endpoint once every 60
| seconds to keep the pre-aggregation creation queries in the queue. Failing to
| do this will prevent pre-aggregations from being built.
<!-- prettier-ignore-end -->

To begin the scheduled refresh, first call the
[`/v1/run-scheduled-refresh`][ref-restapi-sched-refresh] endpoint. The endpoint
will return `{ "finished": false }` whilst the pre-aggregations are being built;
once they are successfully built, the response will change to:

```json
{
  "finished": true
}
```

### Cube Store

Unfortunately, Cube Store currently cannot be run using serverless platforms; we
recommend using [Cube Cloud][link-cube-cloud] which provides a similar
"serverless" experience instead. If you prefer self-hosting, you can use a PaaS
such as [AWS ECS][aws-ecs] or [AWS EKS][aws-eks]. More instructions can be found
in the [Running in Production page under Caching][ref-caching-prod].

## Security

### Networking

To run Cube.js within a VPC, add a `vpc` property to the `serverless.yml`:

```yaml
provider:
  ...
  vpc:
    securityGroupIds:
      - sg-12345678901234567 # Add your DB and Redis security groups here
    subnetIds:
      # Add any subnets with access to your DB, Redis and the Internet
      - subnet-12345678901234567
```

### Use JSON Web Tokens

Cube.js can be configured to use industry-standard JSON Web Key Sets for
securing its API and limiting access to data. To do this, we'll define the
relevant options on our Cube.js API instance:

<!-- prettier-ignore-start -->
[[warning |]]
| If you have cubes that use `SECURITY_CONTEXT` in their `sql` property, then
| you must configure [`scheduledRefreshContexts`][ref-config-sched-ref-ctx] so
| the refresh workers can correctly create pre-aggregations.
<!-- prettier-ignore-end -->

```yaml
provider:
  ...
  environment:
    ...
    CUBEJS_JWK_URL: https://cognito-idp.<AWS_REGION>.amazonaws.com/<USER_POOL_ID>/.well-known/jwks.json
    CUBEJS_JWT_AUDIENCE: <APPLICATION_URL>
    CUBEJS_JWT_ISSUER: https://cognito-idp.<AWS_REGION>.amazonaws.com/<USER_POOL_ID>
    CUBEJS_JWT_ALGS: RS256
    CUBEJS_JWT_CLAIMS_NAMESPACE: <CLAIMS_NAMESPACE>
...
```

## Monitoring

All Cube.js logs can be found in the [AWS CloudWatch][aws-cloudwatch] log group
for the Serverless project.

## Update to the latest version

Find the latest stable release version (currently `v%CURRENT_VERSION`) [from
npm][link-cubejs-sls-npm]. Then update your `package.json` to use the version:

```json
{
  "dependencies": {
    "@cubejs-backend/serverless-aws": "%CURRENT_VERSION",
    "@cubejs-backend/serverless": "%CURRENT_VERSION"
  }
}
```

[aws-cloudwatch]: https://aws.amazon.com/cloudwatch/
[aws-ec2]: https://aws.amazon.com/ec2/
[aws-ecs]: https://aws.amazon.com/ecs/
[aws-eks]: https://aws.amazon.com/eks/
[aws-lambda]: https://aws.amazon.com/lambda/
[aws-redis]: https://aws.amazon.com/elasticache/redis/
[link-aws]: https://aws.amazon.com/
[link-sls]: https://www.serverless.com/
[link-cube-cloud]: https://cubecloud.dev
[link-cubejs-sls-npm]: https://www.npmjs.com/package/@cubejs-backend/serverless
[link-docker-app]: https://www.docker.com/products/docker-app
[ref-caching-prod]: /caching/running-in-production
[ref-config-sched-ref-ctx]: /config#options-reference-scheduled-refresh-contexts
[ref-restapi-sched-refresh]: /rest-api#api-reference-v-1-run-scheduled-refresh
