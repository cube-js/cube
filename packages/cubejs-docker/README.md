<p align="center"><a href="https://cube.dev"><img src="https://raw.githubusercontent.com/cube-js/cube/master/.github/assets/cube-core-logo.png" alt="Cube Core — Open-Source Semantic Layer" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://docs.cube.dev) •
[Blog](https://cube.dev/blog) • [Slack](https://slack.cube.dev) • [X](https://twitter.com/the_cube_dev)

# Supported tags and respective `Dockerfile` links

- `<version>` (`v<major>.<minor>.<patch>`, `v<major>.<minor>`, `v<major>`), `latest` —
  [`latest.Dockerfile`][link-dockerfile-latest]
- `<version>-jdk`, `jdk` — with a JDK for JDBC-based
  drivers, [`latest-debian-jdk.Dockerfile`][link-dockerfile-jdk]
- `dev` — built from the `master` branch, not for production use

[link-dockerfile-latest]: https://github.com/cube-js/cube/blob/master/packages/cubejs-docker/latest.Dockerfile
[link-dockerfile-jdk]: https://github.com/cube-js/cube/blob/master/packages/cubejs-docker/latest-debian-jdk.Dockerfile

# Quick reference

Where to get help? Check out our [Slack][link-slack] or [Stack
Overflow][link-stack-overflow].

Found an issue? File it on our [GitHub Issues][link-github-issues] page.

Supported architectures: `amd64`, `arm64`

Need the documentation? Check out the [Cube documentation
website][link-cube-docs].

[link-slack]: https://slack.cube.dev/
[link-stack-overflow]: https://stackoverflow.com/questions/tagged/cube.js
[link-github-issues]: https://github.com/cube-js/cube/issues
[link-cube-docs]: https://docs.cube.dev

# What is Cube Core?

Cube Core is the open-source semantic layer. Define metrics, dimensions, joins,
and access rules once in code, then expose them through SQL, REST, and GraphQL
APIs to anything downstream — BI tools, custom applications, or AI agents.

Cube Core works with all SQL data sources, including cloud data warehouses like
Snowflake, Databricks, and BigQuery; query engines like Presto and Amazon
Athena; and application databases like Postgres. It has a built-in relational
caching engine, Cube Store, to provide sub-second latency and high concurrency
for API requests.

# How to use this image

The image expects your data model and configuration in `/cube/conf`. Mount your
project folder there.

## Start Cube in development mode

```bash
docker run -p 4000:4000 -p 15432:15432 \
  -v ${PWD}:/cube/conf \
  -e CUBEJS_DEV_MODE=true \
  cubejs/cube:latest
```

Then open `http://localhost:4000` to continue setup in Playground. The SQL API
is available on port `15432`.

> **Development mode is an authentication bypass.** Never expose it to the
> internet or use it in production. See
> [`CUBEJS_DEV_MODE`][link-cube-dev-mode] for details.

## Start Cube in production mode

```bash
docker run -d -p 4000:4000 -p 15432:15432 \
  -v ${PWD}:/cube/conf \
  -e CUBEJS_DB_TYPE=<DB_TYPE> \
  -e CUBEJS_DB_HOST=<HOST> \
  -e CUBEJS_DB_NAME=<DB_NAME> \
  -e CUBEJS_DB_USER=<USER> \
  -e CUBEJS_DB_PASS=<PASS> \
  -e CUBEJS_API_SECRET=<API_SECRET> \
  -e CUBEJS_PG_SQL_PORT=15432 \
  -e CUBEJS_SQL_USER=<SQL_USER> \
  -e CUBEJS_SQL_PASSWORD=<SQL_PASSWORD> \
  cubejs/cube:<version>
```

Pin a specific version tag instead of `latest` in production. For all supported
options, see [Environment Variables][link-cube-env-vars].

A production deployment also needs a refresh worker and a Cube Store cluster
for pre-aggregations. See [Deployment][link-cube-deployment] for a complete
Docker Compose example.

[link-cube-dev-mode]: https://docs.cube.dev/reference/configuration/environment-variables#cubejs_dev_mode
[link-cube-env-vars]: https://docs.cube.dev/reference/configuration/environment-variables
[link-cube-deployment]: https://docs.cube.dev/cube-core/deployment

# License

Cube is [Apache 2.0 licensed](https://github.com/cube-js/cube/blob/master/packages/cubejs-docker/LICENSE).
