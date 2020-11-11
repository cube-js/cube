---
title: Production Checklist
permalink: /production-checklist
category: Deployment
menuOrder: 2
---

This is a checklist for configuring and securing Cube.js for a production deployment.

## Enable Production Mode

When running Cube.js in production make sure `NODE_ENV` is set to `production`.
Some platforms, such as Heroku, do it by default.
In this mode Cube.js unsecured development server and Playground will be disabled by default because there's a security risk serving those in production environments.
Production Cube.js servers can be accessed only with [REST API](rest-api) and Cube.js frontend libraries.

## Set up Redis

Cube.js requires [Redis](https://redis.io/), in-memory data structure store, to run in production.

It uses Redis for query caching and queue.
Set `REDIS_URL` environment variable to provide Cube.js with Redis connection. In case your Redis instance has password, please set password via `REDIS_PASSWORD` environment variable.
Make sure, your Redis allows at least 15 concurrent connections.
Set `REDIS_TLS` env variable to `true` if you want to enable secure connection.

[[warning | Note]]
| Cube.js server instances used by same tenant environments should have same Redis instances. Otherwise they will have different query queues which can lead to incorrect pre-aggregation states and intermittent data access errors.

### Redis Pool

If `REDIS_URL` is provided Cube.js, will create Redis pool with 2 min and 1000 max of concurrent connections by default.
`CUBEJS_REDIS_POOL_MIN` and `CUBEJS_REDIS_POOL_MAX` environment variables can be used to tweak pool size.
No pool behavior with each connection created on demand can be achieved with `CUBEJS_REDIS_POOL_MAX=0` setting.

If your `CUBEJS_REDIS_POOL_MAX` too low you may see `TimeoutError: ResourceRequest timed out` errors.
As a rule of a thumb you need to have `Queue Size * Number of tenants` concurrent connections to ensure best performance possible.
If you use clustered deployments please make sure you have enough connections for all Cube.js server instances.
Lower number of connections still can work however Redis becomes performance bottleneck in this case.

### Running without Redis

If you want to run Cube.js in production without redis you can use `CUBEJS_CACHE_AND_QUEUE_DRIVER=memory` env setting.

[[warning | Note]]
| Serverless and clustered deployments can't be run without Redis as it's used to manage querying queue.

## Set up Pre-aggregations Storage

If you are using [external pre-aggregations](pre-aggregations#external-pre-aggregations) you need to set up and configure external pre-aggregations storage.

Currently, we recommend using MySQL for external pre-aggregations storage. You'd
need to modify the MySQL default configuration to optimize it for the pre-aggregations ingestion and serving. The final configuration may vary depending on the specific use case.

## Set up Refresh Worker

If you are using [scheduled pre-aggregations](pre-aggregations#scheduled-refresh) we recommend running a separate Cube.js worker instance to refresh scheduled pre-aggregations in the background.

```bash
# set the env var to true to signal Cube.js instance to act as a refresh worker
CUBEJS_SCHEDULED_REFRESH_TIMER=true
```

## Enable HTTPS

Production APIs should be served over HTTPS to be secure over the network.

Cube.js doesn't handle SSL/TLS for your API. To serve your API on HTTPS URL you should use reverse proxy, like Nginx, Kong, Caddy, etc., or the cloud provider's load balancer SSL termination features.

### Nginx Sample Configuration

Below you can find the sample `nginx.conf` to proxy requests to Cube.js. To learn how to set up SSL with Nginx please refer to [Nginx docs](https://nginx.org/en/docs/http/configuring_https_servers.html).

```jsx
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

