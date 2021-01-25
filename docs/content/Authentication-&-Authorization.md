---
title: Authentication & Authorization
permalink: /security
category: Authentication & Authorization
menuOrder: 1
---

In Cube.js, authorization (or access control) is based on the **security context**. The diagram below shows how it works during the request processing in Cube.js:


<p
  style="text-align: center"
>
  <img
  src="https://raw.githubusercontent.com/statsbotco/cube.js/master/docs/content/authentication-overview.png"
  style="border: none"
  width="80%"
  />
</p>

Authentication is handled outside of Cube.js. Your authentication server issues JWTs to your client application, which, when sent as part of the request, are verified and decoded by Cube.js to get security context claims to evaluate access control rules.

For access control or authorization, Cube.js allows you to define granular access control rules for every cube in your data schema. Cube.js uses the request and the security context claims in the JWT token to generate a SQL query, which includes row-level constraints from the access control rules.

Cube.js uses [JSON Web Tokens (JWT)][link-jwt] which should be passed in the
`Authorization: <JWT>` header to authenticate requests.

JWTs can also be used to pass additional information about the user, known as a [security context][link-security-context]. It must be stored inside the **u** namespace. It will be accessible in the
[SECURITY_CONTEXT][link-security-context] object in the Data Schema and in [securityContext][link-securitycontext] variable which is used to support [Multitenancy][link-multitenancy].

In the example
below **user_id**, located inside the **u** namespace, will be passed inside the
security context and will be accessible in the [SECURITY_CONTEXT][link-security-context] object.

```json
{
  "sub": "1234567890",
  "iat": 1516239022,
  "u": {
    "user_id": 131
  }
}
```

[link-jwt]: https://jwt.io/
[link-security-context]: /cube#context-variables-security-context
[link-securitycontext]: /config#securitycontext
[link-multitenancy]: /multitenancy-setup


Authentication is handled outside of Cube.js. A typical use case would be:

1. A web server serves an HTML page containing the Cube.js client, which needs
   to communicate securely with the Cube.js API.
2. The web server should generate a JWT with an expiry to achieve this. The
   server could include the token in the HTML it serves or provide the token to
   the frontend via an XHR request, which is then stored it in local storage or
   a cookie.
3. The JavaSript client is initialized using this token, and includes it in
   calls to the Cube.js API.

[link-rest-api]: /rest-api
[link-cubejs-client-core-ref]: /@cubejs-client-core#cubejs

[[info | ]]
| **In development mode, the token is not required for authorization**, but you can still use it to [pass a security context][link-security-context].

[link-security-context]: /security#security-context

## Generating Tokens

Authentication tokens are generated based on your API secret. Cube.js CLI
generates an API Secret when a project is scaffolded and saves this value in the
`.env` file as `CUBEJS_API_SECRET`.

You can generate two types of tokens:

- Without security context, which will mean that all users will have the same
  data access permissions.
- With security context, which will allow you to implement role-based security
  models where users will have different levels of access to data.

[[info | ]]
| It is considered best practice to use an `exp` expiration claim to limit the lifetime of your public tokens. [Learn more in the JWT docs][link-jwt-docs].

[link-jwt-docs]:
  https://github.com/auth0/node-jsonwebtoken#token-expiration-exp-claim

You can find a library for JWT generation for your programming language
[here](https://jwt.io/#libraries-io).

In Node.js, the following code shows how to generate a token which will expire
in 30 days. We recommend using the `jsonwebtoken` package for this.

```javascript
const jwt = require('jsonwebtoken');
const CUBE_API_SECRET = 'secret';

const cubejsToken = jwt.sign({}, CUBE_API_SECRET, { expiresIn: '30d' });
```

Then, in a web server or cloud function, create a route which generates and
returns a token. In general, you will want to protect the URL that generates
your token using your own user authentication and authorization:

```javascript
app.use((req, res, next) => {
  if (!req.user) {
    res.redirect('/login');
    return;
  }
  next();
});

app.get('/auth/cubejs-token', (req, res) => {
  res.json({
    // Take note: cubejs expects the JWT payload to contain an object!
    token: jwt.sign({ u: req.user }, process.env.CUBEJS_API_SECRET, {
      expiresIn: '1d',
    }),
  });
});
```

Then, on the client side, (assuming the user is signed in), fetch a token from
the web server:

```javascript
let apiTokenPromise;

const cubejsApi = cubejs(
  () => {
    if (!apiTokenPromise) {
      apiTokenPromise = fetch(`${API_URL}/auth/cubejs-token`)
        .then((res) => res.json())
        .then((r) => r.token);
    }
    return apiTokenPromise;
  },
  {
    apiUrl: `${API_URL}/cubejs-api/v1`,
  }
);
```

You can optionally store this token in local storage or in a cookie, so that you
can then use it to query the Cube.js API.

## Security Context

A "security context" is a verified set of claims about the current user that the
Cube.js server can use to ensure that users only have access to the data that
they are authorized to access. You can provide a security context by passing the
`u` param in the JSON payload that you pass to your JWT signing function. For
example if you want to pass the user ID in the security context you could create
a token with this json structure:

```json
{
  "u": { "user_id": 42 }
}
```

In this case, the `{ "user_id": 42 }` object will be accessible as
[SECURITY_CONTEXT][link-security-context] in the Cube.js Data Schema.

[link-user-context]: /cube#context-variables-security-context

The Cube.js server expects the context to be an object. If you don't provide an
object as the JWT payload, you will receive an error of the form
`Cannot create proxy with a non-object as target or handler`.

Consider the following example. We want to show orders only for customers who
own these orders. The `orders` table has a `user_id` column, which we can use to
filter the results.

```javascript
cube(`Orders`, {
  sql: `SELECT * FROM public.orders WHERE ${SECURITY_CONTEXT.user_id.filter('user_id')}`,

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

const cubejsToken = jwt.sign({ u: { user_id: 42 } }, CUBEJS_API_SECRET, {
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

Cube.js also allows you to provide your own JWT verification logic by
setting a [`checkAuth()`][link-check-auth-ref] function in the `cube.js`
configuration file. This function is expected to verify a JWT and
assigns its' claims to the security context.

[link-check-auth-ref]: /config#options-reference-check-auth

[[warning | Note]]
| Previous versions of Cube.js allowed setting a `checkAuthMiddleware()`
| parameter, which is now deprecated.
| We advise [migrating to a newer version of Cube.js][link-migrate-cubejs].

[link-migrate-cubejs]: /configuration/overview#migration-from-express-to-docker-template

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
