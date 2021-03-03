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
the Data Schema and in [`securityContext`][ref-config-sec-ctx] variable which is
used to support [Multitenancy][link-multitenancy].

In the example below **user_id** will be passed inside the security context and
will be accessible in the [`SECURITY_CONTEXT`][ref-schema-sec-ctx] object.

```json
{
  "sub": "1234567890",
  "iat": 1516239022,
  "user_id": 131
}
```

In this case, the object above will be accessible as the
[`SECURITY_CONTEXT`][ref-schema-sec-ctx] global variable in the Cube.js Data
Schema.

The Cube.js server expects the context to be an object. If you don't provide an
object as the JWT payload, you will receive an error of the form
`Cannot create proxy with a non-object as target or handler`.

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

## Custom authentication

Cube.js also allows you to provide your own JWT verification logic by setting a
[`checkAuth()`][link-check-auth-ref] function in the `cube.js` configuration
file. This function is expected to verify a JWT and assigns its' claims to the
security context.

<!-- prettier-ignore-start -->
[[warning | Note]]
| Previous versions of Cube.js allowed setting a `checkAuthMiddleware()`
| parameter, which is now deprecated. We advise [migrating to a newer version
| of Cube.js][link-migrate-cubejs].
<!-- prettier-ignore-end -->

For example, if you're using AWS Cognito:

```javascript
const jwt = require('jsonwebtoken');
const fetch = require('node-fetch');

module.exports = {
  checkAuth: async (req, auth) => {
    // Replace `region` and `userPoolId` with your own
    const jwks = await fetch(
      'https://cognito-idp.{region}.amazonaws.com/{userPoolId}/.well-known/jwks.json'
    ).then((r) => r.json());
    const decoded = jwt.decode(auth, { complete: true });
    const jwk = _.find(jwks.keys, (x) => x.kid === decoded.header.kid);
    const pem = jwkToPem(jwk);
    req.securityContext = jwt.verify(auth, pem);
  },
};
```

[link-auth0-jwks]:
  https://auth0.com/docs/tokens/json-web-tokens/json-web-key-sets
[link-check-auth-ref]: /config#options-reference-check-auth
[link-migrate-cubejs]:
  /configuration/overview#migration-from-express-to-docker-template
[link-multitenancy]: /multitenancy-setup
[ref-config-sec-ctx]: /config#request-context-security-context
[ref-schema-sec-ctx]: /cube#context-variables-security-context
