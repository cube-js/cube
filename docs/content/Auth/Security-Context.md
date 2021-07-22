---
title: Security Context
permalink: /security/context
category: Authentication & Authorization
menuOrder: 2
---

Your authentication server issues JWTs to your client application, which, when
sent as part of the request, are verified and decoded by Cube.js to get security
context claims to evaluate access control rules. Inbound JWTs are decoded and
verified using industry-standard [JSON Web Key Sets (JWKS)][link-auth0-jwks].

For access control or authorization, Cube.js allows you to define granular
access control rules for every cube in your data schema. Cube.js uses both the
request and security context claims in the JWT token to generate a SQL query,
which includes row-level constraints from the access control rules.

JWTs sent to Cube.js should be passed in the `Authorization: <JWT>` header to
authenticate requests.

JWTs can also be used to pass additional information about the user, known as a
**security context**. A security context is a verified set of claims about the
current user that the Cube.js server can use to ensure that users only have
access to the data that they are authorized to access.

It will be accessible in the [`SECURITY_CONTEXT`][ref-schema-sec-ctx] object in
the Data Schema and as the [`securityContext`][ref-config-sec-ctx] property
inside the [`COMPILE_CONTEXT`][ref-cubes-compile-ctx] global, which is used to
support [multi-tenant deployments][link-multitenancy].

## Using SECURITY_CONTEXT

In the example below `user_id`, `sub` and `iat` will be injected into the
security context and will be accessible from the
[`SECURITY_CONTEXT`][ref-schema-sec-ctx] global variable in the Cube.js Data
Schema.

```json
{
  "sub": "1234567890",
  "iat": 1516239022,
  "user_id": 131
}
```

<!-- prettier-ignore-start -->
[[warning |]]
| Cube.js expects the context to be an object. If you don't provide an object
| as the JWT payload, you will receive the following error:
| ```
| Cannot create proxy with a non-object as target or handler
| ```
<!-- prettier-ignore-end -->

Consider the following example. We want to show orders only for customers who
own these orders. The `orders` table has a `user_id` column, which we can use to
filter the results.

```javascript
cube(`Orders`, {
  sql: `SELECT * FROM public.orders WHERE ${SECURITY_CONTEXT.user_id.filter(
    'user_id'
  )}`,

  measures: {
    count: {
      type: `count`,
    },
  },
});
```

Now, we can generate an API Token with a security context including the user's
ID:

```javascript
const jwt = require('jsonwebtoken');
const CUBE_API_SECRET = 'secret';

const cubejsToken = jwt.sign({ user_id: 42 }, CUBEJS_API_SECRET, {
  expiresIn: '30d',
});
```

Using this token, we authorize our request to the Cube.js API by passing it in
the Authorization HTTP header.

```bash
curl \
 -H "Authorization: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1Ijp7ImlkIjo0Mn0sImlhdCI6MTU1NjAyNTM1MiwiZXhwIjoxNTU4NjE3MzUyfQ._8QBL6nip6SkIrFzZzGq2nSF8URhl5BSSSGZYp7IJZ4" \
 -G \
 --data-urlencode 'query={"measures":["Orders.count"]}' \
 http://localhost:4000/cubejs-api/v1/load
```

And Cube.js with generate the following SQL:

```sql
SELECT
  count(*) "orders.count"
  FROM (
    SELECT * FROM public.orders WHERE user_id = 42
  ) AS orders
LIMIT 10000
```

## Using COMPILE_CONTEXT

In the example below `user_id`, `company_id`, `sub` and `iat` will be injected
into the security context and will be accessible in both the
[`SECURITY_CONTEXT`][ref-schema-sec-ctx] and
[`COMPILE_CONTEXT`][ref-cubes-compile-ctx] global variables in the Cube.js Data
Schema.

<!-- prettier-ignore-start -->
[[info |]]
| `COMPILE_CONTEXT` is used by Cube.js at schema compilation time, which allows
| changing the underlying dataset completely; whereas `SECURITY_CONTEXT` is
| used at query execution time, which simply filters the dataset with a `WHERE`
| clause. [More information on these differences can be found
| here][ref-sec-ctx-vs-compile-ctx].
<!-- prettier-ignore-end -->

```json
{
  "sub": "1234567890",
  "iat": 1516239022,
  "user_id": 131,
  "company_id": 500
}
```

With the same JWT payload as before, we can modify schemas before they are
compiled. The following schema will ensure users only see results for their
`company_id` in a multi-tenant deployment:

```javascript
const {
  securityContext: { company_id },
} = COMPILE_CONTEXT;

cube(`Orders`, {
  sql: `SELECT * FROM ${company_id}.orders`,

  measures: {
    count: {
      type: `count`,
    },
  },
});
```

## Usage with Pre-Aggregations

To generate pre-aggregations that are security context dependent, [configure
`scheduledRefreshContexts` in your `cube.js` configuration
file][ref-config-sched-refresh].

## Testing during development

During development, it is often useful to be able to edit the security context
to test access control rules. The [Developer
Playground][ref-devtools-playground] allows you to set your own JWTs, or you can
build one from a JSON object.

[link-auth0-jwks]:
  https://auth0.com/docs/tokens/json-web-tokens/json-web-key-sets
[link-multitenancy]: /multitenancy-setup
[ref-config-sched-refresh]: /config#options-reference-scheduled-refresh-contexts
[ref-config-sec-ctx]: /config#request-context-security-context
[ref-schema-sec-ctx]: /schema/reference/cube#context-variables-security-context
[ref-cubes-compile-ctx]:
  https://cube.dev/docs/cube#context-variables-compile-context
[ref-sec-ctx-vs-compile-ctx]:
  /multitenancy-setup#security-context-vs-multitenant-compile-context
[ref-devtools-playground]:
  /dev-tools/dev-playground#editing-the-security-context
