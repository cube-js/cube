---
title: Config
permalink: /config
category: Configuration
subCategory: Reference
menuOrder: 3
---

Cube.js can be configured both via environment variables and by providing
configuration options in the `cube.js` file.

Example of setting a custom logger in the `cube.js` file.

```javascript
module.exports = {
  logger: (msg, params) => {
    console.log(`${msg}: ${JSON.stringify(params)}`);
  },
};
```

## Options Reference

You can provide the following configuration options to Cube.js.

```typescript
interface CubejsConfiguration {
  dbType: string | ((context: RequestContext) => string);
  schemaPath: string;
  basePath: string;
  webSocketsBasePath: string;
  logger: (msg: string, params: object) => any;
  driverFactory: (context: DriverContext) => BaseDriver | Promise<BaseDriver>;
  contextToAppId: (context: RequestContext) => string;
  contextToOrchestratorId: (context: RequestContext) => string;
  repositoryFactory: (context: RequestContext) => SchemaFileRepository;
  checkAuth: (req: ExpressRequest, authorization: string) => any;
  queryRewrite: (query: object, context: RequestContext) => object;
  preAggregationsSchema: string | ((context: RequestContext) => string);
  schemaVersion: (context: RequestContext) => string;
  scheduledRefreshTimer: boolean | number;
  scheduledRefreshTimeZones: string[];
  scheduledRefreshContexts: () => Promise<object[]>;
  extendContext: (req: ExpressRequest) => any;
  compilerCacheSize: number;
  maxCompilerCacheKeepAlive: number;
  updateCompilerCacheKeepAlive: boolean;
  allowUngroupedWithoutPrimaryKey: boolean;
  telemetry: boolean;
  http: {
    cors: {
      methods: string | string[];
      origin: string;
      allowedHeaders: string | string[];
      exposedHeaders: string | string[];
      credentials: boolean;
      maxAge: number;
      preflightContinue: boolean;
      optionsSuccessStatus: number;
    };
  };
  jwt: {
    jwkUrl?: ((payload: any) => string) | string;
    key?: string;
    algorithms?: string[];
    issuer?: string[];
    audience?: string;
    subject?: string;
    claimsNamespace?: string;
  };
  externalDbType: string | ((context: RequestContext) => string);
  externalDriverFactory: (
    context: RequestContext
  ) => BaseDriver | Promise<BaseDriver>;
  cacheAndQueueDriver: 'memory' | 'redis';
  orchestratorOptions:
    | OrchestratorOptions
    | ((context: RequestContext) => OrchestratorOptions);
  allowJsDuplicatePropsInSchema: boolean;
}

interface OrchestratorOptions {
  redisPrefix: string;
  queryCacheOptions: {
    refreshKeyRenewalThreshold: number;
    backgroundRenew: boolean;
    queueOptions: QueueOptions;
  };
  preAggregationsOptions: {
    queueOptions: QueueOptions;
  };
}

interface QueueOptions {
  concurrency: number;
  continueWaitTimeout: number;
  executionTimeout: number;
  orphanedTimeout: number;
  heartBeatInterval: number;
}

interface RequestContext {
  securityContext: object;
  requestId: string;
}

interface DriverContext extends RequestContext {
  dataSource: string;
}

interface SchemaFileRepository {
  dataSchemaFiles(): Promise<FileContent[]>;
}

interface FileContent {
  fileName: string;
  content: string;
}
```

### dbType

Either `String` or `Function` could be passed. Providing a `Function` allows to
dynamically select a database type depending on the user's context. It is
usually used in [Multitenancy Setup][ref-multitenancy].

If no option is passed, Cube.js will lookup for environment variable
`CUBEJS_DB_TYPE` to resolve `dbType`.

Called only once per [`appId`][ref-opts-ctx-to-appid].

### schemaPath

Path to schema files. The default value is `/schema`.

### basePath

[REST API](/rest-api) base path. The default value is `/cubejs-api`.

### webSocketsBasePath

The base path for the websockets server. By default, the WebSockets server will
run on the root path.

### logger

A function to setup a custom logger. It accepts the following arguments:

- `message`: Cube.js Backend event message
- `params`: Parameters of the call

```javascript
module.exports = {
  logger: (msg, params) => {
    console.log(`${msg}: ${JSON.stringify(params)}`);
  },
};
```

### driverFactory

Set a custom database driver. The function accepts context object as an argument
to allow dynamically loading database drivers, which is usually used in
[Multitenancy Applications][ref-multitenancy].

Called once per [`dataSourceId`][ref-opts-ctx-to-datasourceid]. Can return a
`Promise` which resolves to a driver.

```javascript
const PostgresDriver = require('@cubejs-backend/postgres-driver');

module.exports = {
  driverFactory: ({ dataSource }) =>
    new PostgresDriver({ database: dataSource }),
};
```

### contextToAppId

It is a [Multitenancy Setup][ref-multitenancy] option.

`contextToAppId` is a function to determine an App ID which is used as caching
key for various in-memory structures like schema compilation results, connection
pool, etc.

Called on each request.

```javascript
module.exports = {
  contextToAppId: ({ securityContext }) =>
    `CUBEJS_APP_${securityContext.user_id}`,
};
```

### contextToOrchestratorId

`contextToOrchestratorId` is a function to determine a caching key for Query
Orchestrator instance. Query Orchestrator instance holds database connections,
execution queues, pre-aggregation table caches. By default, returns the same
value as `contextToAppId`.

Override it only in case multiple tenants should share the same execution queue
and database connections while having different schemas instead of default Query
Orchestrator per tenant strategy.

Called on each request.

```javascript
module.exports = {
  contextToAppId: ({ securityContext }) =>
    `CUBEJS_APP_${securityContext.tenantId}_${securityContext.user_id}`,
  contextToOrchestratorId: ({ securityContext }) =>
    `CUBEJS_APP_${securityContext.tenantId}`,
};
```

### repositoryFactory

This option allows to customize the repository for Cube.js data schema files. It
is a function, which accepts a context object and can dynamically select
repositories with schema files based on
[`SchemaFileRepository`][ref-schemafilerepo] contract. Learn more about it in
[Multitenancy guide][ref-multitenancy].

Called only once per [`appId`][ref-opts-ctx-to-appid].

```javascript
const FileRepository = require('@cubejs-backend/server-core/core/FileRepository');

// using built-in SchemaFileRepository implementation and supplying the path to schema files
module.exports = {
  repositoryFactory: ({ securityContext }) =>
    new FileRepository(`schema/${securityContext.appId}`),
};

// supplying your own SchemaFileRepository implementation to return array of files
module.exports = {
  repositoryFactory: ({ securityContext }) => {
    return {
      dataSchemaFiles: async () =>
        await Promise.resolve([
          { fileName: 'file.js', content: 'contents of file' },
        ]),
    };
  },
};
```

### checkAuth

Used in both REST and WebSockets API. Can be an `async` functon. Default
implementation parses [JSON Web Tokens (JWT)][link-jwt] in `Authorization`
header and sets payload to `req.securityContext` if it's verified. More
information on how to generate these tokens is [here][ref-sec-ctx].

You can set `req.securityContext = userContextObj` inside the middleware if you
want to customize [`SECURITY_CONTEXT`][ref-cube-ctx-sec-ctx].

Called on each request.

Also, you can use empty `checkAuth` function to disable built-in security. See
an example below.

```javascript
module.exports = {
  checkAuth: (req, auth) => {},
};
```

### queryRewrite

<!-- prettier-ignore-start -->
[[warning | Note]]
| In previous versions of Cube.js, this was called `queryTransformer`.
<!-- prettier-ignore-end -->

This is a security hook to check your query just before it gets processed. You
can use this very generic API to implement any type of custom security checks
your app needs and transform input query accordingly.

Called on each request.

For example you can use `queryRewrite` to add row level security filter where
needed.

```javascript
module.exports = {
  queryRewrite: (query, { securityContext }) => {
    if (securityContext.filterByRegion) {
      query.filters.push({
        member: 'Regions.id',
        operator: 'equals',
        values: [securityContext.regionId],
      });
    }
    return query;
  },
};
```

### preAggregationsSchema

Schema name to use for storing pre-aggregations. For some drivers like MySQL
it's name for pre-aggregation database as there's no database schema concept
there. Either `String` or `Function` could be passed. Providing a `Function`
allows to dynamically set the pre-aggregation schema name depending on the
user's context.

Defaults to `dev_pre_aggregations` in [development mode][ref-development-mode]
and `prod_pre_aggregations` in production.

Can be also set via environment variable `CUBEJS_PRE_AGGREGATIONS_SCHEMA`.

<!-- prettier-ignore-start -->
[[warning |]]
| We **strongly** recommend using different pre-aggregation schemas in development and
| production environments to avoid pre-aggregation tables clashes.
<!-- prettier-ignore-end -->

Called once per [`appId`][ref-opts-ctx-to-appid].

```javascript
// Static usage
module.exports = {
  preAggregationsSchema: `my_pre_aggregations`,
};

// Dynamic usage
module.exports = {
  preAggregationsSchema: ({ securityContext }) =>
    `pre_aggregations_${securityContext.tenantId}`,
};
```

### schemaVersion

Schema version can be used to tell Cube.js schema should be recompiled in case
schema code depends on dynamic definitions fetched from some external database
or API. This method is called on each request however `RequestContext` parameter
is reused per application ID as determined by
[`contextToAppId`][ref-opts-ctx-to-appid]. If the returned string is different,
the schema will be recompiled. It can be used in both multi-tenant and single
tenant environments.

```javascript
const tenantIdToDbVersion = {};

module.exports = {
  schemaVersion: ({ securityContext }) =>
    tenantIdToDbVersion[securityContext.tenantId],
};
```

### scheduledRefreshTimer

<!-- prettier-ignore-start -->
[[warning | Note]]
| This is merely a refresh worker heart beat. It doesn't affect freshness of
| pre-aggregations or refresh keys. Setting this value to `30s` doesn't mean
| pre-aggregations would be refreshed every 30 seconds but rather checked for
| freshness every 30 seconds. Please consult the
| [`refreshKey` documentation][ref-pre-aggregations-refresh-key] on how to set
| refresh intervals for pre-aggregations.
<!-- prettier-ignore-end -->

Cube.js enables background refresh by default. You can specify an interval as a
number in seconds or as a string format e.g. `30s`, `1m`. Can be also set using
`CUBEJS_SCHEDULED_REFRESH_TIMER` env variable.

```javascript
module.exports = {
  scheduledRefreshTimer: 60,
};
```

Learn more about [scheduled refreshes here][ref-caching-up-to-date].

Best practice is to run `scheduledRefreshTimer` in a separate worker Cube.js
instance. For Serverless deployments, [REST API][ref-rest-api-sched-refresh]
should be used instead.

You may also need to configure
[`scheduledRefreshTimeZones`][ref-opts-sched-refresh-tz] and
[`scheduledRefreshContexts`][ref-opts-sched-refresh-ctxs].

### scheduledRefreshTimeZones

All time-based calculations performed within Cube.js are timezone-aware. Using
this property you can specify multiple timezones in [TZ Database
Name][link-wiki-tz] format e.g. `America/Los_Angeles`. The default value is
`UTC`.

```javascript
module.exports = {
  // You can define one or multiple timezones based on your requirements
  scheduledRefreshTimeZones: ['America/Vancouver', 'America/Toronto'],
};
```

This configuration option can be also set using the
`CUBEJS_SCHEDULED_REFRESH_TIMEZONES` environment variable. You can set a
comma-separated list of timezones to refresh in
`CUBEJS_SCHEDULED_REFRESH_TIMEZONES` environment variable. For example:

```bash
CUBEJS_SCHEDULED_REFRESH_TIMEZONES=America/Los_Angeles,UTC
```

### scheduledRefreshContexts

When trying to configure scheduled refreshes for pre-aggregations that use the
`securityContext` inside `contextToAppId` or `contextToOrchestratorId`, you must
also set up `scheduledRefreshContexts`. This will allow Cube.js to generate the
necessary security contexts prior to running the scheduled refreshes.

<!-- prettier-ignore-start -->
[[warning |]]
| Leaving `scheduledRefreshContexts` unconfigured will lead to issues where the
| security context will be `undefined`. This is because there is no way for
| Cube.js to know how to generate a context without the required input.
<!-- prettier-ignore-end -->

```javascript
module.exports = {
  // scheduledRefreshContexts should return an array of `securityContext`s
  scheduledRefreshContexts: async () => [
    {
      securityContext: {
        myappid: 'demoappid',
        bucket: 'demo',
      },
    },
    {
      securityContext: {
        myappid: 'demoappid2',
        bucket: 'demo2',
      },
    },
  ],
};
```

### extendContext

Option to extend the `RequestContext` with custom values. This method is called
on each request. Can be async.

The function should return an object which gets appended to the
[`RequestContext`][ref-opts-req-ctx]. Make sure to register your value using
[`contextToAppId`][ref-opts-ctx-to-appid] to use cache context for all possible
values that your extendContext object key can have.

```javascript
module.exports = {
  contextToAppId: (context) => `CUBEJS_APP_${context.activeOrganization}`,
  extendContext: (req) => {
    return { activeOrganization: req.headers.activeOrganization };
  },
};
```

You can use the custom value from extend context in your data schema like this:

```javascript
const { activeOrganization } = COMPILE_CONTEXT;

cube(`Users`, {
  sql: `SELECT * FROM users where organization_id=${activeOrganization}`,
});
```

### compilerCacheSize

Maximum number of compiled schemas to persist with in-memory cache. Defaults to
250, but optimum value will depend on deployed environment. When the max is
reached, will start dropping the least recently used schemas from the cache.

### maxCompilerCacheKeepAlive

Maximum length of time in ms to keep compiled schemas in memory. Default keeps
schemas in memory indefinitely.

### updateCompilerCacheKeepAlive

Providing `updateCompilerCacheKeepAlive: true` keeps frequently used schemas in
memory by reseting their `maxCompilerCacheKeepAlive` every time they are
accessed.

### allowUngroupedWithoutPrimaryKey

Providing `allowUngroupedWithoutPrimaryKey: true` disables primary key inclusion
check for `ungrouped` queries.

### telemetry

Cube.js collects high-level anonymous usage statistics for servers started in
development mode. It doesn't track any credentials, schema contents or queries
issued. This statistics is used solely for the purpose of constant cube.js
improvement.

You can opt out of it any time by setting `telemetry` option to `false` or,
alternatively, by setting `CUBEJS_TELEMETRY` environment variable to `false`.

```javascript
module.exports = {
  telemetry: false,
};
```

### http

#### cors

CORS settings for the Cube.js REST API can be configured by providing an object
with options [from here][link-express-cors-opts].

### jwt

#### jwkUrl

The URL from which JSON Web Key Sets (JWKS) can be retrieved. Can also be set
using `CUBEJS_JWK_URL`.

#### key

A JSON string that represents a cryptographic key. Similar to `API_SECRET`. Can
also be set using `CUBEJS_JWT_KEY`.

#### algorithms

[Any supported algorithm for decoding JWTs][gh-jsonwebtoken-algs]. Can also be
set using `CUBEJS_JWT_ALGS`.

#### issuer

An issuer value which will be used to enforce the [`iss` claim from inbound
JWTs][link-jwt-ref-iss]. Can also be set using `CUBEJS_JWT_ISSUER`.

#### audience

An audience value which will be used to enforce the [`aud` claim from inbound
JWTs][link-jwt-ref-aud]. Can also be set using `CUBEJS_JWT_AUDIENCE`.

#### subject

A subject value which will be used to enforce the [`sub` claim from inbound
JWTs][link-jwt-ref-sub]. Can also be set using `CUBEJS_JWT_SUBJECT`.

#### claimsNamespace

A namespace within the decoded JWT under which any custom claims can be found.
Can also be set using `CUBEJS_JWT_CLAIMS_NAMESPACE`.

### externalDbType

Should be used in conjunction with
[`externalDriverFactory`](#external-driver-factory) option. Either `String` or
`Function` could be passed. Providing a `Function` allows you to dynamically
select a database type depending on the user's context. It is usually used in
[Multitenancy Setup][ref-multitenancy].

Called only once per [`appId`][ref-opts-ctx-to-appid].

### externalDriverFactory

Set database driver for external rollup database. Please refer to [External
Rollup][ref-preagg-ext-rollup] documentation for more guidance. The function
accepts a context object as an argument to allow dynamically loading database
drivers, which is usually used for [Multitenant deployments][ref-multitenancy].

Called once per [`appId`][ref-opts-ctx-to-appid]. Can return a `Promise` that
resolves to a driver.

```javascript
const MySQLDriver = require('@cubejs-backend/mysql-driver');

module.exports = {
  externalDbType: 'mysql',
  externalDriverFactory: () =>
    new MySQLDriver({
      host: process.env.CUBEJS_EXT_DB_HOST,
      database: process.env.CUBEJS_EXT_DB_NAME,
      port: process.env.CUBEJS_EXT_DB_PORT,
      user: process.env.CUBEJS_EXT_DB_USER,
      password: process.env.CUBEJS_EXT_DB_PASS,
    }),
};
```

### cacheAndQueueDriver

The cache and queue driver to use for the Cube.js deployment. Defaults to
`memory` in development, `redis` in production.

### orchestratorOptions

<!-- prettier-ignore-start -->
[[warning | ]]
| We **strongly** recommend leaving these options set to the defaults. Changing these values can result in application instability and/or downtime.
<!-- prettier-ignore-end -->

You can pass this object to set advanced options for Cube.js Query Orchestrator.

| Option                                       | Description                                                                                                                                                                                                                                                                                                                                                                                                               | Default Value           |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------- |
| redisPrefix                                  | Prefix to be set an all Redis keys                                                                                                                                                                                                                                                                                                                                                                                        | `STANDALONE`            |
| rollupOnlyMode                               | When enabled, an error will be thrown if a query can't be served from a pre-aggregation (rollup)                                                                                                                                                                                                                                                                                                                          | `false`                 |
| queryCacheOptions                            | Query cache options for DB queries                                                                                                                                                                                                                                                                                                                                                                                        | `{}`                    |
| queryCacheOptions.refreshKeyRenewalThreshold | Time in seconds to cache the result of [refreshKey][ref-cube-refresh-key] check                                                                                                                                                                                                                                                                                                                                           | `defined by DB dialect` |
| queryCacheOptions.backgroundRenew            | Controls whether to wait in foreground for refreshed query data if `refreshKey` value has been changed. Refresh key queries or pre-aggregations are never awaited in foreground and always processed in background unless cache is empty. If `true` it immediately returns values from cache if available without [refreshKey][ref-cube-refresh-key] check to renew in foreground. Default value before 0.15.0 was `true` | `false`                 |
| queryCacheOptions.queueOptions               | Query queue options for DB queries                                                                                                                                                                                                                                                                                                                                                                                        | `{}`                    |
| preAggregationsOptions                       | Query cache options for pre-aggregations                                                                                                                                                                                                                                                                                                                                                                                  | `{}`                    |
| preAggregationsOptions.queueOptions          | Query queue options for pre-aggregations                                                                                                                                                                                                                                                                                                                                                                                  | `{}`                    |
| preAggregationsOptions.externalRefresh       | When running a separate instance of Cube.js to refresh pre-aggregations in the background, this option can be set on the API instance to prevent it from trying to check for rollup data being current - it won't try to create or refresh them when this option is `true`                                                                                                                                                | `false`                 |

To set options for `queryCache` and `preAggregations`, set an object with key
queueOptions. `queryCacheOptions` are used while querying database tables, while
`preAggregationsOptions` settings are used to query pre-aggregated tables.

```javascript
const queueOptions = {
  concurrency: 3,
};

module.exports = {
  orchestratorOptions: {
    queryCacheOptions: {
      refreshKeyRenewalThreshold: 30,
      backgroundRenew: true,
      queueOptions,
    },
    preAggregationsOptions: { queueOptions },
  },
};
```

## QueueOptions

Timeout and interval options' values are in seconds.

| Option              | Description                                                                                                                                    | Default Value |
| ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- | ------------- |
| concurrency         | Maximum number of queries to be processed simultaneosly. For drivers with connection pool `CUBEJS_DB_MAX_POOL` should be adjusted accordingly. | `2`           |
| continueWaitTimeout | Long polling interval                                                                                                                          | `5`           |
| executionTimeout    | Total timeout of single query                                                                                                                  | `600`         |
| orphanedTimeout     | Query will be marked for cancellation if not requested during this period.                                                                     | `120`         |
| heartBeatInterval   | Worker heartbeat interval. If `4*heartBeatInterval` time passes without reporting, the query gets cancelled.                                   | `30`          |

## RequestContext

`RequestContext` object is filled by context data on a HTTP request level.

### securityContext

Defined as `req.securityContext` which should be set by
[`checkAuth`][ref-opts-checkauth]. Default implementation of
[`checkAuth`][ref-opts-checkauth] uses [JWT Security Token][ref-sec] payload and
sets it to `req.securityContext`.

## SchemaFileRepository

The `SchemaFileRepository` contract defines an async `dataSchemaFiles` function
which returns the files to compile for a schema. Returned by
[repositoryFactory][ref-repofactory].
`@cubejs-backend/server-core/core/FileRepository` is the default implementation
of the `SchemaFileRepository` contract which accepts
[schemaPath][ref-schemapath] in the constructor.

```javascript
class ApiFileRepository {
  async dataSchemaFiles() {
    const fileContents = await callExternalApiForFileContents();
    return [{ fileName: 'apiFile', content: fileContents }];
  }
}

module.exports = {
  repositoryFactory: ({ securityContext }) => new ApiFileRepository(),
};
```

### allowJsDuplicatePropsInSchema

Boolean to enable or disable a check duplicate property names in all objects of
a schema. The default value is `false`, and it is means the compiler would use
the additional transpiler for check duplicates.

[gh-jsonwebtoken-algs]:
  https://github.com/auth0/node-jsonwebtoken#algorithms-supported
[link-express-cors-opts]:
  https://expressjs.com/en/resources/middleware/cors.html#configuration-options
[link-jwt]: https://jwt.io/
[link-jwt-ref-iss]: https://tools.ietf.org/html/rfc7519#section-4.1.1
[link-jwt-ref-sub]: https://tools.ietf.org/html/rfc7519#section-4.1.2
[link-jwt-ref-aud]: https://tools.ietf.org/html/rfc7519#section-4.1.3
[link-wiki-tz]: https://en.wikipedia.org/wiki/Tz_databas
[ref-caching-up-to-date]: /caching#keeping-cache-up-to-date
[ref-cube-refresh-key]: /schema/reference/cube#parameters-refresh-key
[ref-cube-ctx-sec-ctx]:
  /schema/reference/cube#context-variables-security-context
[ref-multitenancy]: /multitenancy-setup
[ref-ext-driverfactory]: #external-driver-factory
[ref-opts-req-ctx]: #request-context
[ref-opts-checkauth]: #options-reference-check-auth
[ref-opts-ctx-to-appid]: #options-reference-context-to-app-id
[ref-opts-ctx-to-datasourceid]: #options-reference-context-to-data-source-id
[ref-opts-sched-refresh-ctxs]: #options-reference-scheduled-refresh-contexts
[ref-opts-sched-refresh-tz]: #options-reference-scheduled-refresh-time-zones
[ref-preagg-ext-rollup]: /schema/reference/pre-aggregations#external-rollup
[ref-repofactory]: #repositoryFactory
[ref-schemafilerepo]: #SchemaFileRepository
[ref-schemapath]: #schemaPath
[ref-sec]: /security
[ref-sec-ctx]: /security/context
[ref-rest-api]: /rest-api
[ref-rest-api-sched-refresh]: /rest-api#api-reference-v-1-run-scheduled-refresh
[ref-development-mode]: /overview#development-mode
[ref-pre-aggregations-refresh-key]:
  /schema/reference/pre-aggregations#refresh-key
