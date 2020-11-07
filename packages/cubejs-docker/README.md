<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://cube.dev/docs) • [Blog](https://cube.dev/blog) • [Slack](https://slack.cube.dev) • [Twitter](https://twitter.com/thecubejs)

# Supported tags

- [latest](https://github.com/cube-js/cube.js/blob/master/packages/cubejs-docker/latest.Dockerfile) - Latest stable release (recommended)
- [dev](https://github.com/cube-js/cube.js/blob/master/packages/cubejs-docker/dev.Dockerfile) - Latest development release from master branch.

# What is Cube.js?

Cube.js is an open-source analytical API platform. It is primarily used to build internal business intelligence tools or add customer-facing analytics to existing applications.

Cube.js was designed to work with Serverless Query Engines like AWS Athena and Google BigQuery. Multi-stage querying approach makes it suitable for handling trillions of data points. Most modern RDBMS work with Cube.js as well and can be tuned for adequate performance.

Unlike others, it is not a monolith application, but a set of modules, which does one thing well. Cube.js provides modules to run transformations and modeling in data warehouse, querying and caching, managing API gateway and building UI on top of that.

# How to use this image

```sh
docker pull cubejs/cube:latest
```

### License

Cube.js Docker is [Apache 2.0 licensed](./LICENSE).
