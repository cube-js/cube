---
title: Production Checklist
permalink: /deployment/production-checklist
category: Deployment
menuOrder: 2
---

This is a checklist for configuring and securing Cube.js for a production
deployment.

## Disable Development Mode

When running Cube.js in production environments, make sure development mode is
disabled. Running Cube.js in development mode in a production environment can
lead to security vulnerabilities. You can read more on the differences between
[production and development mode here][link-cubejs-dev-vs-prod].

[link-cubejs-dev-vs-prod]: /configuration/overview#development-mode

<!-- prettier-ignore-start -->
[[info | Note]]
| Development mode is disabled by default.
<!-- prettier-ignore-end -->

```bash
# Set this to false or leave unset to disable development mode
CUBEJS_DEV_MODE=false
```

## Set up Redis

Cube.js requires [Redis](https://redis.io/), an in-memory data structure store,
to run in production.

It uses Redis for query caching and queue. Set the `REDIS_URL` environment
variable to allow Cube.js to connect to Redis. If your Redis instance also has a
password, please set it via the `REDIS_PASSWORD` environment variable. Set the
`REDIS_TLS` environment variable to `true` if you want to enable SSL-secured
connections. Ensure your Redis cluster allows at least 15 concurrent
connections.

### Redis Sentinel

Redis provides functionality for high availability through
[`Redis Sentinel`][redis-sentinel].

For Redis Sentinel support, the npm package [`ioredis`][gh-ioredis] needs to be
used instead of[`redis`][gh-node-redis]. This is done by setting the
`CUBEJS_REDIS_USE_IOREDIS` environment variable to `true`. Then set
`CUBEJS_REDIS_URL` to the
`redis+sentinel://localhost:26379,otherhost:26479/mymaster/5` to allow Cube.js
to connect to the Redis Sentinel.

[redis-sentinel]: https://redis.io/topics/sentinel
[gh-ioredis]: https://github.com/luin/ioredis
[gh-node-redis]: https://github.com/NodeRedis/node-redis

<!-- prettier-ignore-start -->
[[warning | Note]]
| Cube.js server instances used by same tenant environments should have same
| Redis instances. Otherwise they will have different query queues which can
| lead to incorrect pre-aggregation states and intermittent data access errors.
<!-- prettier-ignore-end -->

### Redis Pool

If `REDIS_URL` is provided Cube.js, will create a Redis connection pool with a
minimum of 2 and maximum of 1000 concurrent connections, by default. The
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

## Set up Pre-aggregations Storage

If you are using [external pre-aggregations][link-pre-aggregations], you need to
set up and configure external pre-aggregations storage.

[link-pre-aggregations]: /pre-aggregations#external-pre-aggregations

By default, Cube.js will use `prod_pre_aggregations` as the schema name for
storing pre-aggregations. This behavior can be modified by the
`CUBEJS_PRE_AGGREGATIONS_SCHEMA` environent variable; see the [Environment
Variables][ref-env-vars-general] page for more details.

[ref-env-vars-general]: /reference/environment-variables#general

Currently, we recommend using MySQL for external pre-aggregations storage. There
is some additional MySQL configuration required to optimize for pre-aggregation
ingestion and serving. The final configuration may vary depending on the
specific use case.

## Set up Refresh Worker

To refresh in-memory cache and [scheduled
pre-aggregations][link-scheduled-refresh] in the background, we recommend
running a separate Cube.js refresh worker instance. This allows your main
Cube.js instance to continue to serve requests with high availability.

[link-scheduled-refresh]: /pre-aggregations#scheduled-refresh

```bash
# Set to true so a Cube.js instance acts as a refresh worker
CUBEJS_SCHEDULED_REFRESH_TIMER=true
```

For Serverless deployments, use the [Run Scheduled Refresh endpoint of the REST
API][ref-api-scheduled-refresh] instead of a refresh worker.

[ref-api-scheduled-refresh]: /rest-api#api-reference-v-1-run-scheduled-refresh

## Enable HTTPS

Production APIs should be served over HTTPS to be secure over the network.

Cube.js doesn't handle SSL/TLS for your API. To serve your API on HTTPS URL you
should use a reverse proxy, like [NGINX][link-nginx], [Kong][link-kong],
[Caddy][link-caddy] or your cloud provider's load balancer SSL termination
features.

[link-nginx]: https://www.nginx.com/
[link-kong]: https://konghq.com/kong/
[link-caddy]: https://caddyserver.com/

### NGINX Sample Configuration

Below you can find a sample `nginx.conf` to proxy requests to Cube.js. To learn
how to set up SSL with NGINX please refer to [NGINX docs][link-nginx-docs].

[link-nginx-docs]: https://nginx.org/en/docs/http/configuring_https_servers.html

```nginx
server {
  listen 80;
  server_name cube.my-domain.com;

  location / {
    proxy_pass http://localhost:4000/;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
  }
}
```

## Configure JWKS

If you're using JWTs, you can configure Cube.js to correctly decode them and
inject their contents into the [Security Context][ref-sec-ctx]. Add your
authentication provider's configuration under [the `jwt` property of your
`cube.js` configuration file][ref-config-jwt].

[ref-sec-ctx]: /security/context
[ref-config-jwt]: /config#options-reference-jwt

## Set up health checks

Cube.js provides [Kubernetes-API compatible][link-k8s-healthcheck-api] health
check (or probe) endpoints that indicate the status of the deployment. Configure
your monitoring service of choice to use the [`/readyz`][ref-api-readyz] and
[`/livez`][ref-api-livez] API endpoints so you can check on the Cube.js
deployment's health and be alerted to any issues.

[link-k8s-healthcheck-api]:
  https://kubernetes.io/docs/reference/using-api/health-checks/
[ref-api-readyz]: /rest-api#api-reference-readyz
[ref-api-livez]: /rest-api#api-reference-livez
