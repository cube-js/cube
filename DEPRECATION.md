# Deprecation

This page provides an overview of features that are deprecated in Cube.js.
Changes in packaging, and supported (Linux) distributions are not included. To
learn about end of support for Linux distributions, refer to the
[changelog](CHANGELOG.md).

## Feature Deprecation Policy

As changes are made to Cube.js, there may be times when existing features need
to be removed or replaced with newer features. Before an existing feature is
removed it is marked as "deprecated" within the documentation and remains in
Cube.js for at least one stable release unless specified explicitly otherwise.
After that time it may be removed.

Users are expected to take note of the list of deprecated features each release
and plan their migration away from those features, and (if applicable) towards
the replacement features as soon as possible.

## Deprecated Features

The table below provides an overview of the current status of deprecated
features:

- **Deprecated**: the feature is marked "deprecated" and should no longer be
  used. The feature may be removed, disabled, or change behavior in a future
  release. The _"Deprecated"_ column contains the release in which the feature
  was marked deprecated, whereas the _"Remove"_ column contains a tentative
  release in which the feature is to be removed.
- **Removed**: the feature was removed, disabled, or hidden. Refer to the linked
  section for details. Some features are "soft" deprecated, which means that
  they remain functional for backward compatibility, and to allow users to
  migrate to alternatives. In such cases, a warning may be printed, and users
  should not rely on this feature.

| Status     | Feature                                                                                                                           | Deprecated | Removed   |
|------------|-----------------------------------------------------------------------------------------------------------------------------------|------------|-----------|
| Removed    | [Node.js 8](#nodejs-8)                                                                                                            | v0.22.4    | v0.26.0   |
| Removed    | [`hearBeatInterval`](#hearbeatinterval)                                                                                           | v0.23.8    | June 2021 |
| Removed    | [`CUBEJS_ENABLE_TLS`](#cubejs_enable_tls)                                                                                         | v0.23.11   | v0.26.0   |
| Removed    | [Embedding Cube.js within Express](#embedding-cubejs-within-express)                                                              | v0.24.0    | June 2021 |
| Removed    | [Absolute import for `@cubejs-backend/query-orchestrator`](#absolute-import-for-@cubejs-backendquery-orchestrator)                | v0.24.2    | v0.32.0   |
| Removed    | [`contextToDataSourceId`](#contexttodatasourceid)                                                                                 | v0.25.0    | v0.25.0   |
| Removed    | [Absolute import for `@cubejs-backend/server-core`](#absolute-import-for-@cubejs-backendserver-core)                              | v0.25.4    | v0.32.0   |
| Removed    | [Absolute import for `@cubejs-backend/schema-compiler`](#absolute-import-for-@cubejs-backendschema-compiler)                      | v0.25.21   | v0.32.0   |
| Removed    | [`checkAuthMiddleware`](#checkauthmiddleware)                                                                                     | v0.26.0    | v0.36.0   |
| Removed    | [Node.js 10](#nodejs-10)                                                                                                          | v0.26.0    | v0.29.0   |
| Removed    | [Node.js 15](#nodejs-15)                                                                                                          | v0.26.0    | v0.32.0   |
| Removed    | [`USER_CONTEXT`](#user_context)                                                                                                   | v0.26.0    | v0.36.0   |
| Deprecated | [`authInfo`](#authinfo)                                                                                                           | v0.26.0    |           |
| Removed    | [Prefix Redis environment variables with `CUBEJS_`](#prefix-redis-environment-variables-with-cubejs_)                             | v0.27.0    | v0.36.0   |
| Removed    | [Node.js 12](#nodejs-12)                                                                                                          | v0.29.0    | v0.32.0   |
| Deprecated | [`CUBEJS_EXTERNAL_DEFAULT` and `CUBEJS_SCHEDULED_REFRESH_DEFAULT`](#cubejs_external_default-and-cubejs_scheduled_refresh_default) | v0.30.0    |           |
| Deprecated | [Using external databases for pre-aggregations](#using-external-databases-for-pre-aggregations)                                   | v0.30.0    |           |
| Deprecated | [`dbType`](#dbtype)                                                                                                               | v0.30.30   |           |
| Removed    | [Serverless Deployments](#serverless-deployments)                                                                                 | v0.31.64   | v0.35.0   |
| Removed    | [Node.js 14](#nodejs-14)                                                                                                          | v0.32.0    | v0.35.0   |
| Removed    | [Using Redis for in-memory cache and queue](#using-redis-for-in-memory-cache-and-queue)                                           | v0.32.0    | v0.36.0   |
| Deprecated | [`SECURITY_CONTEXT`](#security_context)                                                                                           | v0.33.0    |           |
| Deprecated | [`running_total` measure type](#running_total-measure-type)                                                                       | v0.33.39   |           |
| Removed    | [Top-level `includes` parameter in views](#top-level-includes-parameter-in-views)                                                 | v0.34.34   | v1.3.0    |
| Removed    | [Node.js 16](#nodejs-16)                                                                                                          | v0.35.0    | v0.36.0   |
| Removed    | [MySQL-based SQL API](#mysql-based-sql-api)                                                                                       | v0.35.0    | v0.35.0   |
| Removed    | [`initApp` hook](#initapp-hook)                                                                                                   | v0.35.0    | v0.35.0   |
| Removed    | [`/v1/run-scheduled-refresh` REST API endpoint](#v1run-scheduled-refresh-rest-api-endpoint)                                       | v0.35.0    | v0.36.0   |
| Removed    | [Node.js 18](#nodejs-18)                                                                                                          | v0.36.0    | v1.3.0    |
| Deprecated | [`CUBEJS_SCHEDULED_REFRESH_CONCURRENCY`](#cubejs_scheduled_refresh_concurrency)                                                   | v1.2.7 |           |
| Deprecated | [Node.js 20](#nodejs-20)                                                                                                          | v1.3.0    |           |

### Node.js 8

**Removed in Release: v0.26.0**

Node.js 8 reached [End of Life on December 31, 2019][link-nodejs-eol]. This
means no more updates. Please upgrade to Node.js 10 or higher.

### `hearBeatInterval`

**Deprecated in Release: v0.23.8**

This option for [`@cubejs-client/ws-transport`][link-hearbeatinterval] has been
replaced by `heartBeatInterval`.

[link-hearbeatinterval]:
  https://cube.dev/docs/@cubejs-client-ws-transport#web-socket-transport-hear-beat-interval

### `CUBEJS_ENABLE_TLS`

**Removed in Release: v0.26.0**

We no longer recommend setting TLS options via Cube.js. Developers should set up
TLS on a load balancer or reverse proxy instead. [Read more
here][link-enable-https].

[link-enable-https]:
  https://cube.dev/docs/deployment/production-checklist#enable-https

### Embedding Cube.js within Express

**Deprecated in Release: v0.24.0**

Embedding Cube.js into Express applications is deprecated due to performance and
reliability considerations. [Read more about this change
here][link-cube-docker].

Developers are encouraged to [migrate to the new `cube.js` configuration
file][link-migration] and deploy Cube.js as a microservice (or multiple
microservices, if necessary).

[link-cube-docker]: https://cube.dev/blog/cubejs-loves-docker
[link-migration]:
  https://cube.dev/docs/configuration/overview#migrating-from-express-to-docker

### Absolute import for `@cubejs-backend/query-orchestrator`

**Removed in Release: v0.32.0**

Absolute imports are highly dependent on a path, and all API becomes public. We
now provide a public API from the package directly.

Deprecated:

```javascript
const BaseDriver = require("@cubejs-backend/query-orchestrator/driver/BaseDriver");
```

You should use:

```javascript
const { BaseDriver } = require("@cubejs-backend/query-orchestrator");
```

### `contextToDataSourceId`

**Removed in Release: v0.25.0**

The `contextToDataSourceId` option in the `cube.js` configuration file has been
replaced by [`contextToOrchestratorId`][link-contexttoorchestratorid]. Prior to
this change, multi-tenant setups were forced to share a Query Orchestrator
instance. Now orchestrator instances can be shared by Cube.js instances and
across different tenants, if need be. Single-tenant setups should consider
removing the `contextToDataSourceId` property completely.

[link-contexttoorchestratorid]:
  https://cube.dev/docs/config#options-reference-context-to-orchestrator-id

### Absolute import for `@cubejs-backend/server-core`

**Removed in Release: v0.32.0**

Absolute imports are highly dependent on a path, and all API becomes public. We
now provide a public API from the package directly.

Deprecated:

```javascript
const CubejsServerCore = require("@cubejs-backend/server-core");
```

You should use:

```javascript
const { CubejsServerCore } = require("@cubejs-backend/server-core");
```

### Absolute import for `@cubejs-backend/schema-compiler`

**Removed in Release: v0.32.0**

Absolute imports are highly dependent on a path, and all API becomes public. We
now provide a public API from the package directly.

Deprecated:

```javascript
const BaseQuery = require("@cubejs-backend/schema-compiler/adapter/BaseQuery");
```

You should use:

```javascript
const { BaseQuery } = require("@cubejs-backend/schema-compiler");
```

### `checkAuthMiddleware`

**Removed in Release: v0.36.0**

The `checkAuthMiddleware` option was tightly bound to Express,
[which has been deprecated](#embedding-cubejs-within-express). Since Cube.js
supports HTTP **and** WebSockets as transports, we want our authentication API
to not rely on transport-specific details. We now recommend using
[`checkAuth`][ref-checkauth] as a transport-agnostic method of authentication.
This means the same authentication logic can be reused for both HTTP and
WebSockets transports.

If you are using custom authorization, please take a [look at the
documentation][link-custom-auth]

[link-custom-auth]: https://cube.dev/docs/security#custom-authentication
[ref-checkauth]: https://cube.dev/docs/config#options-reference-check-auth

### Node.js 10

**Removed in Release: v0.29.0**

Node.js 10 reached [End of Life on April 30, 2021][link-nodejs-eol]. This means
no more updates. Please upgrade to Node.js 12 or higher.

### `USER_CONTEXT`

**Removed in Release: v0.36.0**

`USER_CONTEXT` has been renamed to `SECURITY_CONTEXT`.

You should use:

```js
cube(`visitors`, {
  sql: `select * from visitors WHERE ${SECURITY_CONTEXT.source.filter(
    "source"
  )}`,
});
```

### `authInfo`

**Deprecated in Release: v0.26.0**

The `authInfo` parameter to `checkAuth` no longer wraps the decoded JWT under
the `u` property. It has also been renamed to
[`securityContext`][ref-security-context]. Additionally, the security context
claims are now populated from the root payload instead of the `u` property.

Old shape of `authInfo`:

```json
{
  "sub": "1234567890",
  "u": { "user_id": 131 }
}
```

New shape of `authInfo`:

```json
{
  "sub": "1234567890",
  "user_id": 131
}
```

[ref-security-context]: https://cube.dev/docs/security/context

Deprecated:

```js
const server = new CubejsServer({
 checkAuth: async (req, auth) => { // Notice how we're using the `u` property in `jwt.verify()` and assigning the result to `req.authInfo` req.authInfo = jwt.verify({ u: auth }, pem); }, contextToAppId: ({ authInfo }) => `APP_${authInfo.userId}`, preAggregationsSchema: ({ authInfo }) => `pre_aggregations_${authInfo.userId}`,});
```

You should use:

```js
const server = new CubejsServer({
 checkAuth: async (req, auth) => { // We're now using directly assigning the result of `jet.verify()` to the `securityContext` property req.securityContext = jwt.verify(auth, pem); }, // And here we're now using the `securityContext` parameter contextToAppId: ({ securityContext }) => `APP_${securityContext.userId}`, // And the same here preAggregationsSchema: ({ securityContext }) => `pre_aggregations_${securityContext.userId}`,});
```

### Prefix Redis environment variables with `CUBEJS_`

**Removed in Release: v0.36.0**

### Node.js 15

**Removed in Release: v0.29.0**

### Node.js 12

**Removed in Release: v0.32.0**

### Using non-Cube Store databases as external database

**Deprecated in Release: v0.29.0**

Cube no longer supports using databases such as MySQL and Postgres as external
databases. [Please switch to using Cube Store][link-running-in-prod] as it is a
more robust and reliable solution.

[link-running-in-prod]: https://cube.dev/docs/caching/running-in-production

### `CUBEJS_EXTERNAL_DEFAULT` and `CUBEJS_SCHEDULED_REFRESH_DEFAULT`

**Deprecated in Release: v0.30.0**

The `CUBEJS_EXTERNAL_DEFAULT` and `CUBEJS_SCHEDULED_REFRESH_DEFAULT` environment
variables are now marked as deprecated; they were introduced to smooth the
migration to Cube Store and are no longer necessary.

### Using external databases for pre-aggregations

**Deprecated in Release: v0.30.0**

Using external databases for pre-aggregations is now deprecated, and we strongly
recommend [using Cube Store as a solution][ref-caching-in-prod].

[ref-caching-in-prod]: https://cube.dev/docs/caching/running-in-production

### `dbType`

**Deprecated in Release: v0.30.30**

Using `dbType` is now deprecated, and we recommend using
[`driverFactory`][self-driver-factory] to return a `DriverConfig` object
instead.

### Serverless Deployments

**Removed in Release: v0.35.0**

Using Serverless deployments with the `@cubejs-backend/serverless` package is
now deprecated; we **strongly** recommend using Docker-based deployments
instead.

### Node.js 14

**Removed in Release: v0.35.0**

### Using Redis for in-memory cache and queue

**Removed in Release: v0.36.0**

Cube Store is now the default cache and queue engine, [replacing
Redis](https://cube.dev/blog/replacing-redis-with-cube-store). Please migrate to
[Cube Store](https://cube.dev/blog/how-you-win-by-using-cube-store-part-1).

### `SECURITY_CONTEXT`

**Deprecated in Release: v0.33.0**

The `SECURITY_CONTEXT` context variable is deprecated. Use
[`query_rewrite`](https://cube.dev/docs/reference/configuration/config#query_rewrite)
instead.

### `running_total` measure type

**Deprecated in Release: v0.33.39**

The `running_total` measure type is now deprecated, and we recommend using
[`rolling_window`](https://cube.dev/docs/product/data-modeling/reference/measures#rolling_window)
to calculate running totals instead.

### Top-level `includes` parameter in views

**Removed in Release: v1.3.0**

The top-level `includes` parameter is now removed. Please always use the
`includes` parameter within [`cubes` and `join_path`
parameters](https://cube.dev/docs/reference/data-model/view#cubes) so you can
explicitly control the join path.

### Node.js 16

**Removed in Release: v0.36.0**

[link-nodejs-eol]: https://github.com/nodejs/Release#end-of-life-releases

### MySQL-based SQL API

**Removed in release: v0.35.0**

Early prototype of the MySQL-based SQL API is removed in favor of the Postgres-compatible
[SQL API](https://cube.dev/docs/product/apis-integrations/sql-api), together with the
`CUBEJS_SQL_PORT` environment variable.

### `initApp` hook

**Removed in release: v0.35.0**

The `initApp` hook is removed as it's not relevant anymore for Docker-based architecture.

### `/v1/run-scheduled-refresh` REST API endpoint

**Removed in release: v0.36.0**

The `/v1/run-scheduled-refresh` REST API endpoint is deprecated as it's not
relevant anymore for Docker-based architecture. Use the [Orchestration
API](https://cube.dev/docs/product/apis-integrations/orchestration-api) and
`/v1/pre-aggregations/jobs` endpoint instead.

### Node.js 18

**Deprecated in Release: v0.36.0**

Node.js 18 reaches [End of Life on April 30, 2025][link-nodejs-eol]. This means
no more updates. Please upgrade to Node.js 20 or higher.

### `CUBEJS_SCHEDULED_REFRESH_CONCURRENCY`

**Deprecated in Release: v1.2.7**

This environment variable was renamed to [`CUBEJS_SCHEDULED_REFRESH_QUERIES_PER_APP_ID`](https://cube.dev/docs/reference/configuration/environment-variables#cubejs_scheduled_refresh_queries_per_app_id). Please use the new name.

### Node.js 18

**Removed in Release: v1.3.0**

[link-nodejs-eol]: https://github.com/nodejs/Release#end-of-life-releases

### Node.js 20

**Deprecated in Release: v1.3.0**

Node.js 20 is in maintenance mode from [November 22, 2024][link-nodejs-eol]. This means
no more new features, only security updates. Please upgrade to Node.js 22 or higher.
