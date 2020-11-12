<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://cube.dev/docs) • [Blog](https://cube.dev/blog) • [Slack](https://slack.cube.dev) • [Twitter](https://twitter.com/thecubejs)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)
[![GitHub Actions](https://github.com/cube-js/cube.js/workflows/Build/badge.svg)](https://github.com/cube-js/cube.js/actions?query=workflow%3ABuild+branch%3Amaster)

# Cube.js MySql Aurora Serverless Data Api Driver

Pure Javascript MySql Aurora Serverless Data Api driver.

[Learn more](https://github.com/cube-js/cube.js#getting-started)

## Using the Data API for Aurora Serverless

Uses Jeremy Daly's [data-api-client](https://github.com/jeremydaly/data-api-client) for connection to the Data API for Aurora Serverless. This does not require a persistent connection to the database.

[Learn more](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/data-api.html)

## Integration Testing

Uses the `mysql` and [local-data-api](https://hub.docker.com/r/koxudaxi/local-data-api) containers to mock the RDS Data API

### License

Cube.js MySql Serverless Aurora Data Api Driver for is [Apache 2.0 licensed](./LICENSE).
