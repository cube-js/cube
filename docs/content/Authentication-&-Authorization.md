---
title: Authentication & Authorization
permalink: /security
category: Authentication & Authorization
menuOrder: 1
---

Cube.js uses [JSON Web Tokens (JWT)][link-jwt] which should be passed in the
`Authorization` header to authenticate requests. JWTs can also be used for
passing additional information about the user, which can be accessed in the
[USER_CONTEXT][link-user-context] object in the Data Schema.

[link-jwt]: https://jwt.io/
[link-user-context]: /cube#context-variables-user-context

The `Authorization` header is parsed and the JWT's contents set to the
[authInfo][link-authinfo] variable which can be used to support
[Multitenancy][link-multitenancy].

[link-authinfo]: /@cubejs-backend-server-core#authinfo
[link-multitenancy]: /multitenancy-setup

Cube.js tokens are designed to work well in microservice-based environments. A
typical use case would be:

1. A web server serves an HTML page containing the Cube.js client, which needs
   to communicate securely with the Cube.js API.
2. The web server should generate a JWT with an expiry to achieve this. The
   server could include the token in the HTML it serves or provide the token to
   the frontend via an XHR request, which is then stored it in local storage or
   a cookie.
3. The JavaSript client is initialized using this token, and includes it in
   calls to the Cube.js API.

If you are using the [REST API][link-rest-api] you must pass the API token via
the Authorization Header. The Cube.js JavaScript client accepts an
authentication token as the first argument to the
[`cubejs(authToken, options)`][link-cubejs-client-core-ref] function.

[link-rest-api]: /rest-api
[link-cubejs-client-core-ref]: /@cubejs-client-core#cubejs

**In development mode, the token is not required for authorization**, but you
can still use it to [pass a security context][link-security-context].

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

_It is considered best practice to use an `exp` expiration claim to limit the
life time of your public tokens. [Learn more at JWT docs][link-jwt-docs]._

[link-jwt-docs]:
  https://github.com/auth0/node-jsonwebtoken#token-expiration-exp-claim

### Using the CLI

You can use the Cube.js CLI [`token`][link-cubejs-cli-token-ref] command to
generate an API token.

[link-cubejs-cli-token-ref]: /reference#token

```bash
$ cubejs token -e TOKEN-EXPIRY -s SECRET -p FOO=BAR
```

It is helpful to be able to create an API token with the CLI command for testing
and development purposes, but we strongly recommend programmatically generating
tokens in production.

### Programmatically

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
cube.js server can use to ensure that users only have access to the data that
they are authorized to access. You can provide a security context by passing the
`u` param in the JSON payload that you pass to your JWT signing function. For
example if you want to pass the user ID in the security context you could create
a token with this json structure:

```json
{
  "u": { "id": 42 }
}
```

In this case, the `{ "id": 42 }` object will be accessible as
[USER_CONTEXT][link-user-context] in the Cube.js Data Schema.

[link-user-context]: /cube#context-variables-user-context

The Cube.js server expects the context to be an object. If you don't provide an
object as the JWT payload, you will receive an error of the form
`Cannot create proxy with a non-object as target or handler`.

Consider the following example. We want to show orders only for customers who
own these orders. The `orders` table has a `user_id` column, which we can use to
filter the results.

```javascript
cube(`Orders`, {
  sql: `SELECT * FROM public.orders WHERE ${USER_CONTEXT.id.filter('user_id')}`,

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

const cubejsToken = jwt.sign({ u: { id: 42 } }, CUBEJS_API_SECRET, {
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
    req.authInfo = jwt.verify(auth, pem);
  },
};
```
