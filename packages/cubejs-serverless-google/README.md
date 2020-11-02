<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://cube.dev/docs) • [Blog](https://cube.dev/blog) • [Slack](https://slack.cube.dev) • [Twitter](https://twitter.com/thecubejs)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)
[![GitHub Actions](https://github.com/cube-js/cube.js/workflows/Build/badge.svg)](https://github.com/cube-js/cube.js/actions?query=workflow%3ABuild+branch%3Amaster)

# Cube.js Serverless Google

Cube.js Serverless Google Cloud Platform implementation.

## Getting Started

Create Cube.js Serverless app:

```
$ npm install -g serverless
$ cubejs create -d bigquery -t serverless-google
```

Update all placeholders in just created `serverless.yml`.

## Setup Redis

1. [Create Redis](https://cloud.google.com/memorystore/docs/redis/quickstart-console).
2. [Setup Serverless VPC Access Connector](https://cloud.google.com/functions/docs/connecting-vpc).

## Deploy

```
$ serverless deploy -v
```

As serverless-google-cloudfunctions plugin doesn't support vpc-connector you need to [set it manually](https://cloud.google.com/functions/docs/connecting-vpc#configuring) each time after deploy.

## Logs

```
$ serverless logs -t -f cubejs
$ serverless logs -t -f cubejsProcess
```

[Learn more](https://cube.dev/docs)

### License

Cube.js Serverless AWS is [Apache 2.0 licensed](./LICENSE).
