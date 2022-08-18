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

| Status     | Feature                                                                                                                           | Deprecated | Remove    |
| ---------- | --------------------------------------------------------------------------------------------------------------------------------- | ---------- | --------- |
| Removed    | [Node.js 8](#nodejs-8)                                                                                                            | v0.22.4    | v0.26.0   |
| Deprecated | [`hearBeatInterval`](#hearbeatinterval)                                                                                           | v0.23.8    | June 2021 |
| Removed    | [`CUBEJS_ENABLE_TLS`](#cubejs_enable_tls)                                                                                         | v0.23.11   | v0.26.0   |
| Deprecated | [Embedding Cube.js within Express](#embedding-cubejs-within-express)                                                              | v0.24.0    | June 2021 |
| Deprecated | [Absolute import for `@cubejs-backend/query-orchestrator`](#absolute-import-for-@cubejs-backendquery-orchestrator)                | v0.24.2    | v0.28.0   |
| Removed    | [`contextToDataSourceId`](#contexttodatasourceid)                                                                                 | v0.25.0    | v0.25.0   |
| Deprecated | [Absolute import for `@cubejs-backend/server-core`](#absolute-import-for-@cubejs-backendserver-core)                              | v0.25.4    | v0.30.0   |
| Deprecated | [Absolute import for `@cubejs-backend/schema-compiler`](#absolute-import-for-@cubejs-backendschema-compiler)                      | v0.25.21   | v0.32.0   |
| Deprecated | [`checkAuthMiddleware`](#checkauthmiddleware)                                                                                     | v0.26.0    |           |
| Removed    | [Node.js 10](#nodejs-10)                                                                                                          | v0.26.0    | v0.29.0   |
| Removed    | [Node.js 15](#nodejs-15)                                                                                                          | v0.26.0    | v0.29.0   |
| Deprecated | [`USER_CONTEXT`](#user_context)                                                                                                   | v0.26.0    |           |
| Deprecated | [`authInfo`](#authinfo)                                                                                                           | v0.26.0    |           |
| Deprecated | [Prefix Redis environment variables with `CUBEJS_`](#prefix-redis-environment-variables-with-cubejs_)                             | v0.27.0    |           |
| Deprecated | [Node.js 12](#nodejs-12)                                                                                                          | v0.29.0    |           |
| Deprecated | [`CUBEJS_EXTERNAL_DEFAULT` and `CUBEJS_SCHEDULED_REFRESH_DEFAULT`](#cubejs_external_default-and-cubejs_scheduled_refresh_default) | v0.30.0    |           |
| Deprecated | [Using external databases for pre-aggregations](#using-external-databases-for-pre-aggregations)                                   | v0.30.0    |           |
| Deprecated | [`dbType`](#dbtype)                                                                                                               | v0.30.30   |           |

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

**Deprecated in Release: v0.24.2**

Absolute imports are highly dependent on a path, and all API becomes public. We
now provide a public API from the package directly.

Deprecated:

```javascript
const BaseDriver = require('@cubejs-backend/query-orchestrator/driver/BaseDriver');
```

You should use:

```javascript
const { BaseDriver } = require('@cubejs-backend/query-orchestrator');
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

**Deprecated in Release: v0.25.4**

Absolute imports are highly dependent on a path, and all API becomes public. We
now provide a public API from the package directly.

Deprecated:

```javascript
const CubejsServerCore = require('@cubejs-backend/server-core');
```

You should use:

```javascript
const { CubejsServerCore } = require('@cubejs-backend/server-core');
```

### Absolute import for `@cubejs-backend/schema-compiler`

**Deprecated in Release: v0.25.21**

Absolute imports are highly dependent on a path, and all API becomes public. We
now provide a public API from the package directly.

Deprecated:

```javascript
const BaseQuery = require('@cubejs-backend/schema-compiler/adapter/BaseQuery');
```

You should use:

```javascript
const { BaseQuery } = require('@cubejs-backend/schema-compiler');
```

### `checkAuthMiddleware`

**Deprecated in Release: v0.26.0**

The `checkAuthMiddleware` option was tightly bound to Express,
[which has been deprecated](#embedding-cubejs-within-express). Since Cube.js
supports HTTP **and** WebSockets as transports, we want our authentication API
to not rely on transport-specific details. We now recommend using
[`checkAuth`][ref-checkauth] as a transport-agnostic method of authentication.
This means the same authentication logic can be reused for both HTTP and
Websockets transports.

If you are using custom authorization, please take a [look at the
documentation][link-custom-auth]

[link-custom-auth]: https://cube.dev/docs/security#custom-authentication
[ref-checkauth]: https://cube.dev/docs/config#options-reference-check-auth

### Node.js 10

**Removed in Release: v0.29.0**

Node.js 10 reached [End of Life on April 30, 2021][link-nodejs-eol]. This means
no more updates. Please upgrade to Node.js 12 or higher.

### `USER_CONTEXT`

**Deprecated in Release: v0.26.0**

`USER_CONTEXT` has been renamed to `SECURITY_CONTEXT`.

Deprecated:

```js
cube(`visitors`, {
  sql: `select * from visitors WHERE ${USER_CONTEXT.source.filter('source')}`,
});
```

You should use:

```js
cube(`visitors`, {
  sql: `select * from visitors WHERE ${SECURITY_CONTEXT.source.filter(
    'source'
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

Redis-related environment variables are now prefixed with `CUBEJS_` for
consistency with other environment variables.

**Deprecated in Release: v0.27.0**

Deprecated:

```
REDIS_URL=XXXX
REDIS_PASSWORD=XXX
REDIS_TLS=true
```

You should use:

```
CUBEJS_REDIS_URL=XXXX
CUBEJS_REDIS_PASSWORD=XXX
CUBEJS_REDIS_TLS=true
```

### Node.js 15

**Removed in Release: v0.29.0**

Node.js 15 reached [End of Life on June 1, 2021][link-nodejs-eol]. This means no
more updates. Please upgrade to Node.js 14 or higher.

### Node.js 12

**Deprecated in Release: v0.29.0**

Node.js 12 reached [End of Life on May 19, 2021][link-nodejs-eol]. This means no
more updates. Please upgrade to Node.js 14 or higher.

[link-nodejs-eol]: https://github.com/nodejs/Release#end-of-life-releases

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

### dbType

**Deprecated in Release: v0.30.30**

Using `dbType` is now deprecated, and we recommend using
[`driverFactory`][self-driver-factory] to return a `DriverConfig` object
instead.
