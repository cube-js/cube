---
title: Production Checklist
permalink: /deployment/production-checklist
category: Deployment
menuOrder: 3
---

This is a checklist for configuring and securing Cube.js for a production
deployment.

## Disable Development Mode

When running Cube.js in production environments, make sure development mode is
disabled both on API Instances and Refresh Worker. Running Cube.js in
development mode in a production environment can lead to security
vulnerabilities. You can read more on the differences between [production and
development mode here][link-cubejs-dev-vs-prod].

<!-- prettier-ignore-start -->
[[info | Note]]
| Development mode is disabled by default.
<!-- prettier-ignore-end -->

```bash
# Set this to false or leave unset to disable development mode
CUBEJS_DEV_MODE=false
```

## Set up Refresh Worker

To refresh in-memory cache and [pre-aggregations][ref-schema-ref-preaggs] in the
background, we recommend running a separate Cube.js Refresh Worker instance.
This allows your Cube.js API Instance to continue to serve requests with high
availability.

```bash
# Set to true so a Cube.js instance acts as a refresh worker
CUBEJS_REFRESH_WORKER=true
```

## Set up Cube Store

Cube Store is the purpose-built pre-aggregations storage for Cube.js. Follow the
[instructions here][ref-caching-cubestore] to set it up.

Depending on your database, Cube.js may need to "stage" pre-aggregations inside
your database first before ingesting them into Cube Store. In this case, Cube.js
will require write access to the `prod_pre_aggregations` schema inside your
database. The schema name can be modified by the
`CUBEJS_PRE_AGGREGATIONS_SCHEMA` environment variable; see the [Environment
Variables][ref-env-vars-general] page for more details.

<!-- prettier-ignore-start -->
[[info | Note]]
| You may consider enabling an export bucket which allows Cube.js to build large pre-aggregations in
| a much faster manner. It is currently suppored for BigQuery, Redshift and Snowflake. Check [the relevant documentation for your
| configured database][ref-config-connect-db-notes] to set it up.
<!-- prettier-ignore-end -->

## Set up Redis

Cube.js requires [Redis](https://redis.io/), an in-memory data structure store,
to run in production.

It uses Redis for query caching and queue. Set the `CUBEJS_REDIS_URL`
environment variable to allow Cube.js to connect to Redis. If your Redis
instance also has a password, please set it via the `CUBEJS_REDIS_PASSWORD`
environment variable. Set the `CUBEJS_REDIS_TLS` environment variable to `true`
if you want to enable SSL-secured connections. Ensure your Redis cluster allows
at least 15 concurrent connections.

### Redis Sentinel

Redis provides functionality for high availability through
[`Redis Sentinel`][link-redis-sentinel].

For Redis Sentinel support, the npm package [`ioredis`][gh-ioredis] needs to be
used instead of[`redis`][gh-node-redis]. This is done by setting the
`CUBEJS_REDIS_USE_IOREDIS` environment variable to `true`. Then set
`CUBEJS_REDIS_URL` to the
`redis+sentinel://localhost:26379,otherhost:26479/mymaster/5` to allow Cube.js
to connect to the Redis Sentinel.

<!-- prettier-ignore-start -->
[[warning | Note]]
| Cube.js server instances used by same tenant environments should have same
| Redis instances. Otherwise they will have different query queues which can
| lead to incorrect pre-aggregation states and intermittent data access errors.
<!-- prettier-ignore-end -->

### Redis Pool

If `CUBEJS_REDIS_URL` is provided Cube.js, will create a Redis connection pool
with a minimum of 2 and maximum of 1000 concurrent connections, by default. The
`CUBEJS_REDIS_POOL_MIN` and `CUBEJS_REDIS_POOL_MAX` environment variables can be
used to tweak pool size limits. To disable connection pooling, and instead
create connections on-demand, you can set `CUBEJS_REDIS_POOL_MAX` to 0.

If your maximum concurrent connections limit is too low, you may see
`TimeoutError: ResourceRequest timed out` errors. As a rule of a thumb, you need
to have `Queue Size * Number of tenants` concurrent connections to ensure the
best performance possible. If you use clustered deployments, please make sure
you have enough connections for all Cube.js server instances. A lower number of
connections still can work, however Redis becomes a performance bottleneck in
this case.

### Running without Redis

If you want to run Cube.js in production without Redis, you can use
`CUBEJS_CACHE_AND_QUEUE_DRIVER` environment variable to `memory`.

<!-- prettier-ignore-start -->
[[warning | Note]]
| Serverless and clustered deployments can't be run without Redis as it is used
| to manage the query queue.
<!-- prettier-ignore-end -->

## Secure the deployment

If you're using JWTs, you can configure Cube.js to correctly decode them and
inject their contents into the [Security Context][ref-sec-ctx]. Add your
authentication provider's configuration under [the `jwt` property of your
`cube.js` configuration file][ref-config-jwt], or use [the corresponding
environment variables (`CUBEJS_JWK_URL`,
`CUBEJS_JWT_*`)][ref-config-env-vars-general].

## Set up health checks

Cube.js provides [Kubernetes-API compatible][link-k8s-healthcheck-api] health
check (or probe) endpoints that indicate the status of the deployment. Configure
your monitoring service of choice to use the [`/readyz`][ref-api-readyz] and
[`/livez`][ref-api-livez] API endpoints so you can check on the Cube.js
deployment's health and be alerted to any issues.

[gh-ioredis]: https://github.com/luin/ioredis
[gh-node-redis]: https://github.com/NodeRedis/node-redis
[link-caddy]: https://caddyserver.com/
[link-cubejs-dev-vs-prod]: /configuration/overview#development-mode
[link-k8s-healthcheck-api]:
  https://kubernetes.io/docs/reference/using-api/health-checks/
[link-kong]: https://konghq.com/kong/
[link-nginx]: https://www.nginx.com/
[link-nginx-docs]: https://nginx.org/en/docs/http/configuring_https_servers.html
[link-redis-sentinel]: https://redis.io/topics/sentinel
[ref-config-connect-db-notes]: /connecting-to-the-database#notes
[ref-caching-cubestore]: /caching/running-in-production
[ref-env-vars-general]: /reference/environment-variables#general
[ref-schema-ref-preaggs]: /schema/reference/pre-aggregations
[ref-api-scheduled-refresh]: /rest-api#api-reference-v-1-run-scheduled-refresh
[ref-sec-ctx]: /security/context
[ref-config-jwt]: /config#options-reference-jwt
[ref-config-env-vars-general]: /reference/environment-variables#general
[ref-api-readyz]: /rest-api#api-reference-readyz
[ref-api-livez]: /rest-api#api-reference-livez
