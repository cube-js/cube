<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://cube.dev/docs) •
[Blog](https://cube.dev/blog) • [Slack](https://slack.cube.dev) •
[Discourse](https://forum.cube.dev/) • [Twitter](https://twitter.com/thecubejs)

# Supported tags and respective `Dockerfile` links

Debian based: `<version>`, `latest`, `dev`

Alpine based: `alpine`, `<version>-alpine`

# Quick reference

Where to get help? Check out our [Slack][link-slack], [Stack
Overflow][link-stack-overflow] or [GitHub Discussions][link-github-discussions]
pages.

[link-slack]: https://slack.cube.dev/
[link-stack-overflow]: https://stackoverflow.com/search?q=cube.js
[link-github-discussions]: https://github.com/cube-js/cube.js/discussions

Found an issue? File it on our GitHub Issues page.

Supported architectures: `amd64`

Need the documentation? Check out the [Cube.js documentation
website][link-cubejs-docs].

[link-cubejs-docs]: https://cube.dev/docs

# What is Cube.js?

Cube.js is an open-source analytical API platform. It is primarily used to build
internal business intelligence tools or add customer-facing analytics to
existing applications.

Cube.js was designed to work with Serverless Query Engines like AWS Athena and
Google BigQuery. Multi-stage querying approach makes it suitable for handling
trillions of data points. Most modern RDBMS work with Cube.js as well and can be
tuned for adequate performance.

Unlike others, it is not a monolith application, but a set of modules, which
does one thing well. Cube.js provides modules to run transformations and
modeling in data warehouse, querying and caching, managing API gateway and
building UI on top of that.

# How to use this image

## Start a Cube.js instance

```bash
docker pull cubejs/cube:latest
docker run -p 3000:3000 -p 4000:4000 \
  -e CUBEJS_DEV_MODE=true \
  -e CUBEJS_DB_TYPE=databricks-jdbc \
  -e CUBEJS_DB_NAME=default \
  -e CUBEJS_DB_DATABRICKS_URL="jdbc:spark://dbc-5a7d63cf-e6dd.cloud.databricks.com:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/5928427581414565/0511-154725-hkkg2x4n;AuthMech=3;UID=token;PWD=dapi84bb26088d83d2b1de79bc2c0cbe90c7" \
  -e CUBEJS_DB_DATABRICKS_ACCEPT_POLICY=true \
  -e CUBEJS_API_SECRET=SECRET \
  -v ${PWD}:/cube/conf \
  cube-jdk-cloud:latest
  
  
  docker run -p 3000:3000 -p 4000:4000 \
  -e CUBEJS_DEV_MODE=true \
  -e CUBEJS_DB_TYPE=databricks-jdbc \
  -e CUBEJS_DB_NAME=default \
  -e CUBEJS_DB_DATABRICKS_URL="jdbc:spark://dbc-5a7d63cf-e6dd.cloud.databricks.com:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/5928427581414565/0511-154725-hkkg2x4n;AuthMech=3;UID=token;PWD=dapi84bb26088d83d2b1de79bc2c0cbe90c7" \
  -e CUBEJS_DB_DATABRICKS_ACCEPT_POLICY=true \
  -e CUBEJS_API_SECRET=SECRET \
  -v $(pwd):/cube/conf \
  cube-jdk:latest
```

CUBEJS_DEV_MODE=true
CUBEJS_EXTERNAL_DEFAULT=true
CUBEJS_SCHEDULED_REFRESH_DEFAULT=true
CUBEJS_WEB_SOCKETS=true

CUBEJS_DB_EXPORT_BUCKET_TYPE=s3
CUBEJS_DB_TYPE=databricks-jdbc
CUBEJS_DB_NAME=default
CUBEJS_DB_DATABRICKS_URL=jdbc:spark://dbc-5a7d63cf-e6dd.cloud.databricks.com:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/5928427581414565/0511-154725-hkkg2x4n;AuthMech=3;UID=token;PWD=dapi84bb26088d83d2b1de79bc2c0cbe90c7
CUBEJS_DB_DATABRICKS_ACCEPT_POLICY=true

CUBEJS_DB_EXPORT_BUCKET=s3://cube-test-e2e
CUBEJS_DB_EXPORT_BUCKET_AWS_KEY=AKIAXAMPONEMQ7IFDL6F
CUBEJS_DB_EXPORT_BUCKET_AWS_SECRET=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
CUBEJS_DB_EXPORT_BUCKET_AWS_REGION=us-east-1

The Cube.js Developer Playground will be available at `http://localhost:4000`.
For more information about supported environment variables, please consult the
documentation for [Environment Variables][link-cubejs-env-vars].

[link-cubejs-env-vars]: https://cube.dev/docs/reference/environment-variables

### Using Docker Compose

```yaml
version: '2.2'

services:
  cube:
    image: cubejs/cube:latest
    depends_on:
      - redis
    links:
      - redis
    ports:
      # It's better to use random port binding for 4000/3000 ports
      # without it you will not able to start multiple projects inside docker
      - 4000:4000  # Cube.js API and Developer Playground
      - 3000:3000  # Dashboard app, if created
    env_file: .env
    volumes:
      # If you are going to use own dependencies, for example axios/vault or anything else for getting configuration
      - .:/cube/conf

  redis:
    image: redis:6
    restart: always
```

### License

Cube.js Docker is [Apache 2.0 licensed](./LICENSE).
