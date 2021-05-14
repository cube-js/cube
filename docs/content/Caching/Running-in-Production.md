---
title: Running in Production
permalink: /caching/running-in-production
category: Caching
menuOrder: 4
---

Cube.js makes use of two different kinds of cache:

- Redis, for in-memory storage of query results
- Cube Store for storing pre-aggregations

In development, Cube.js uses in-memory storage on the server. In production, we
**strongly** recommend running Redis as a separate service.

Cube Store is enabled by default when running Cube.js in development mode. In
production, Cube Store **must** run as a separate process. The easiest way to do
this is to use the official Docker images for Cube.js and Cube Store.

<!-- prettier-ignore-start -->
[[info | ]]
| Using Windows? We **strongly** recommend using
| [WSL2 for Windows 10][link-wsl2] to run the following commands.
<!-- prettier-ignore-end -->

You can run Cube Store with Docker with the following command:

```bash
docker run -p 3030:3030 cubejs/cubestore
```

<!-- prettier-ignore-start -->
[[info | ]]
| Cube Store can further be configured via environment variables. To see a
| complete reference, please consult the [Cube Store section of the Environment
| Variables reference][ref-config-env].
<!-- prettier-ignore-end -->

Next, run Cube.js and tell it to connect to Cube Store running on `localhost`
(on the default port `3030`):

```bash
docker run -p 4000:4000 \
  -e CUBEJS_CUBESTORE_HOST=localhost \
  -v ${PWD}:/cube/conf \
  cubejs/cube
```

In the command above, we're specifying `CUBEJS_CUBESTORE_HOST` to let Cube.js
know where Cube Store is running.

You can also use Docker Compose to achieve the same:

```yaml
version: '2.2'
services:
  cubestore:
    image: cubejs/cubestore:latest
    environment:
      - CUBESTORE_REMOTE_DIR=/cube/data
    volumes:
      - .cubestore:/cube/data

  cube:
    image: cubejs/cube:latest
    ports:
      - 4000:4000
    environment:
      - CUBEJS_CUBESTORE_HOST=localhost
      - CUBEJS_CUBESTORE_PORT=3030
    depends_on:
      - cubestore
    links:
      - cubestore
    volumes:
      - ./schema:/cube/conf/schema
```

### Scaling

<!-- prettier-ignore-start -->
[[warning | ]]
| Cube Store _can_ be run in a single instance mode, but this is usually
| unsuitable for production deployments. For high concurrency and data
| throughput, we **strongly** recommend running Cube Store as a cluster of
| multiple instances instead.
<!-- prettier-ignore-end -->

Scaling Cube Store for a higher concurrency is relatively simple when running in
cluster mode. Because [the storage layer](#running-in-production-storage) is
decoupled from the query processing engine, you can horizontally scale your Cube
Store cluster for as much concurrency as you require.

In cluster mode, Cube Store runs two kinds of nodes:

- a single **router** node handles incoming client connections, manages database
  metadata and serves simple queries.
- multiple **worker** nodes which execute SQL queries

The configuration required for each node can be found in the table below. More
information about these variables can be found [in the Environment Variables
reference][ref-config-env].

| Environment Variable    | Specify on Router? | Specify on Worker? |
| ----------------------- | ------------------ | ------------------ |
| `CUBESTORE_SERVER_NAME` | Yes                | Yes                |
| `CUBESTORE_META_PORT`   | Yes                | -                  |
| `CUBESTORE_WORKERS`     | Yes                | -                  |
| `CUBESTORE_WORKER_PORT` | -                  | Yes                |
| `CUBESTORE_META_ADDR`   | -                  | Yes                |

<!-- prettier-ignore-start -->
[[info | ]]
| To fully take advantage of the worker nodes in the cluster, we **strongly**
| recommend using [partitioned pre-aggregations][ref-caching-partitioning].
<!-- prettier-ignore-end -->

A sample Docker Compose stack setting this up might look like:

```yaml
version: '2.2'
services:
  cubestore_router:
    restart: always
    image: cubejs/cubestore:latest
    environment:
      - CUBESTORE_LOG_LEVEL=trace
      - CUBESTORE_SERVER_NAME=cubestore_router:9999
      - CUBESTORE_META_PORT=9999
      - CUBESTORE_WORKERS=cubestore_worker_1:9001,cubestore_worker_2:9001
      - CUBESTORE_REMOTE_DIR=/cube/data
    expose:
      - 9999 # This exposes the Metastore endpoint
      - 3030 # This exposes the HTTP endpoint for CubeJS
    volumes:
      - .cubestore:/cube/data
  cubestore_worker_1:
    restart: always
    image: cubejs/cubestore:latest
    environment:
      - CUBESTORE_SERVER_NAME=cubestore_worker_1:9001
      - CUBESTORE_WORKER_PORT=9001
      - CUBESTORE_META_ADDR=cubestore_router:9999
      - CUBESTORE_REMOTE_DIR=/cube/data
    depends_on:
      - cubestore_router
    expose:
      - 9001
    volumes:
      - .cubestore:/cube/data
  cubestore_worker_2:
    restart: always
    image: cubejs/cubestore:latest
    environment:
      - CUBESTORE_SERVER_NAME=cubestore_worker_2:9001
      - CUBESTORE_WORKER_PORT=9001
      - CUBESTORE_META_ADDR=cubestore_router:9999
      - CUBESTORE_REMOTE_DIR=/cube/data
    depends_on:
      - cubestore_router
    expose:
      - 9001
    volumes:
      - .cubestore:/cube/data
  cube:
    image: cubejs/cube:latest
    ports:
      - 4000:4000
      - 9999
    environment:
      - CUBEJS_CUBESTORE_HOST=cubestore_router
      - CUBEJS_CUBESTORE_PORT=9999
    depends_on:
      - cubestore_router
    volumes:
      - .:/cube/conf
```

### Storage

<!-- prettier-ignore-start -->
[[warning | ]]
| Cube Store can only use one type of remote storage at runtime.
<!-- prettier-ignore-end -->

Cube Store makes use of a separate storage layer for storing metadata as well as
for persisting pre-aggregations as Parquet files. Cube Store can use both AWS S3
and Google Cloud, or if desired, a local path on the server.

A simplified example using AWS S3 might look like:

```yaml
version: '2.2'
services:
  cubestore_router:
    image: cubejs/cubestore:latest
    environment:
      - CUBESTORE_SERVER_NAME=cubestore_router:9999
      - CUBESTORE_META_PORT=9999
      - CUBESTORE_WORKERS=cubestore_worker_1:9001
      - CUBESTORE_S3_BUCKET=<BUCKET_NAME_IN_S3>
      - CUBESTORE_S3_REGION=<BUCKET_REGION_IN_S3>
      - CUBESTORE_AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID>
      - CUBESTORE_AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY>
  cubestore_worker_1:
    image: cubejs/cubestore:latest
    environment:
      - CUBESTORE_SERVER_NAME=cubestore_worker_1:9001
      - CUBESTORE_WORKER_PORT=9001
      - CUBESTORE_META_ADDR=cubestore_router:9999
      - CUBESTORE_S3_BUCKET=<BUCKET_NAME_IN_S3>
      - CUBESTORE_S3_REGION=<BUCKET_REGION_IN_S3>
      - CUBESTORE_AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID>
      - CUBESTORE_AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY>
    depends_on:
      - cubestore_router
```

[link-wsl2]: https://docs.microsoft.com/en-us/windows/wsl/install-win10
[ref-caching-partitioning]: /caching/using-pre-aggregations#partitioning
[ref-config-env]: /reference/environment-variables#cube-store
