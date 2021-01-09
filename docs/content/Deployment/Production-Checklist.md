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

[[info | Note]]
| Development mode is disabled by default.

```bash
# Set this to false or leave unset to disable development mode
CUBEJS_DEV_MODE=false
```

## Set up Cache and Queue
Cube.js requires [Redis](https://redis.io/), an in-memory data structure store, or [DynamoDB](https://aws.amazon.com/dynamodb/), a NoSQL database service, to run in production.

### Redis

It uses Redis for query caching and queue. Set the `REDIS_URL` environment
variable to allow Cube.js to connect to Redis. If your Redis instance also has
a password, please set it via the `REDIS_PASSWORD` environment variable. Set
the `REDIS_TLS` environment variable to `true` if you want to enable
SSL-secured connections. Ensure your Redis cluster allows at least 15
concurrent connections.

[[warning | Note]]
| Cube.js server instances used by same tenant environments should have same
| Redis instances. Otherwise they will have different query queues which can
| lead to incorrect pre-aggregation states and intermittent data access errors.

#### Redis Pool

If `REDIS_URL` is provided Cube.js, will create a Redis connection pool with a
minimum of 2 and maximum of 1000 concurrent connections, by default.
The `CUBEJS_REDIS_POOL_MIN` and `CUBEJS_REDIS_POOL_MAX` environment variables
can be used to tweak pool size limits. To disable connection pooling, and
instead create connections on-demand, you can set `CUBEJS_REDIS_POOL_MAX` to 0.

If your maximum concurrent connections limit is too low, you may see
`TimeoutError: ResourceRequest timed out` errors. As a rule of a thumb, you
need to have `Queue Size * Number of tenants` concurrent connections to ensure
the best performance possible. If you use clustered deployments, please make
sure you have enough connections for all Cube.js server instances. A lower
number of connections still can work, however Redis becomes a performance
bottleneck in this case.

### DynamoDB

Cube.js will use DynamoDB as the cache and queue driver. Set the `CUBEJS_CACHE_TABLE` to your DynamoDB table name. The table must be created with 
```
* partitionKey: pk (string/hash)
* sortKey: sk (string/hash)
* Global secondary index
*   GSI1: -- GSI1 is the index name
*     partitionKey: pk (string/hash as above)
*     sortKey: sk (number/range)
```

### Running without Redis or DynamoDB

If you want to run Cube.js in production without Redis, you can use
`CUBEJS_CACHE_AND_QUEUE_DRIVER` environment variable to `memory`.

[[warning | Note]]
| Serverless and clustered deployments can't be run without Redis as it is used
| to manage the query queue.

## Set up Pre-aggregations Storage

If you are using [external pre-aggregations][link-pre-aggregations], you need
to set up and configure external pre-aggregations storage.

[link-pre-aggregations]: /pre-aggregations#external-pre-aggregations

Currently, we recommend using MySQL for external pre-aggregations storage.
There is some additional MySQL configuration required to optimize for
pre-aggregation ingestion and serving. The final configuration may vary
depending on the specific use case.

## Set up Refresh Worker

To refresh in-memory cache and [scheduled pre-aggregations][link-scheduled-refresh] in the background, we
recommend running a separate Cube.js refresh worker instance. This allows your main Cube.js instance
to continue to serve requests with high availability.

[link-scheduled-refresh]: /pre-aggregations#scheduled-refresh

```bash
# Set to true so a Cube.js instance acts as a refresh worker
CUBEJS_SCHEDULED_REFRESH_TIMER=true
```

For Serverless deployments, use the [Run Scheduled Refresh endpoint of the REST API](rest-api#api-reference-v-1-run-scheduled-refresh) instead of a refresh worker.

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
