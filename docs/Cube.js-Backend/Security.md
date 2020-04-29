---
title: Security
permalink: /security
category: Cube.js Backend
menuOrder: 4
---

Cube.js uses [JSON Web Tokens (JWT)](https://jwt.io/) which passed in `Authorization` header for requests' authorization and also for passing
additional user context, which could be used in the [USER_CONTEXT](cube#context-variables-user-context) object in the Data
Schema.
`Authorization` header is parsed and set to [authInfo](@cubejs-backend-server-core#authinfo) variable which is also can be used for [Multitenancy](multitenancy-setup).

Cube.js tokens are designed to work in micro services environment.
Typical use case would be:

1. There's web server that serves HTML with JS client code that calls cube.js.
2. Web server should generate expirable cube.js tokens and incorporate them as part of HTML or send it over XHR request in exchange of session cookie or other security credentials.
3. JS Client code uses token to call cube.js server API.

If you are using [REST API](rest-api) you need pass API Token via the Authorization Header.
Cube.js Javascript client accepts auth token as a first argument to [cubejs(authToken, options) function](@cubejs-client-core#cubejs).

**In the development environment the token is not required for authorization**, but
you can still use it to [pass a security context](security#security-context).

Cube.js also supports Transport Layer Encryption (TLS) using Node.js native packages. For more information, see [Enabling TLS](security#enabling-tls).

## Generating Token

Auth token is generated based on your API secret. Cube.js CLI generates API Secret on app creation and saves it in `.env` file as `CUBEJS_API_SECRET` variable.

You can generate two types of tokens:
- Without security context. It implies same data access permissions for all users.
- With security context. User or role-based security models can be implemented using this approach.

_It is considered best practice to use `exp` expiration claim to limit life time of your public tokens.
[Learn more at JWT docs](https://github.com/auth0/node-jsonwebtoken#token-expiration-exp-claim)._

### Using CLI

You can use a Cube.js CLI [token](reference#token) command to generate an API token.

```bash
$ cubejs token -e TOKEN-EXPIRY -s SECRET -p FOO=BAR
```

However it is handy to create an API token with CLI command for testing
purposes, we strongly recommend to programmatically generate tokens in production.

### Programmatically

You can find a library for JWT generation for your programming language [here](https://jwt.io/#libraries-io).

Below you can find an example on how to generate an API token in Node.js with
`jsonwebtoken` package:

```javascript
const jwt = require('jsonwebtoken');
const CUBE_API_SECRET='secret';

const cubejsToken = jwt.sign({}, CUBE_API_SECRET, { expiresIn: '30d' });
```

Most often generation of tokens should be served as protected url:

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
    token: jwt.sign({ u: req.user }, process.env.CUBEJS_API_SECRET, { expiresIn: '1d' })
  })
})
```

Then fetched on client side as:

```javascript
let apiTokenPromise;

const cubejsApi = cubejs(() => {
  if (!apiTokenPromise) {
    apiTokenPromise = fetch(`${API_URL}/auth/cubejs-token`)
      .then(res => res.json()).then(r => r.token)
  }
  return apiTokenPromise;
}, {
  apiUrl: `${API_URL}/cubejs-api/v1`
});
```

## Security Context

Security context can be provided by passing `u` param for payload.
For example if you want to pass user id in security context you can create token with payload:
```json
{
  "u": { "id": 42 }
}
```

In this case `{ "id": 42 }` object will be accessible as [USER_CONTEXT](cube#context-variables-user-context) in the Cube.js Data Schema.

The Cube.js server expects the context to be an object. If you don't provide an object as the JWT payload, you will see an error like `Cannot create proxy with a non-object as target or handler`.

Consider the following example. We want to show orders only for
customers, who owns these orders. `orders` table has a `user_id` column, which we
can use to filter the results.

```javascript
cube(`Orders`, {
  sql: `SELECT * FROM public.orders WHERE ${USER_CONTEXT.id.filter('user_id')}`,

  measures: {
    count: {
      type: `count`
    }
  }
});
```

Now, we can generate an API Token with user ID:

```javascript
const jwt = require('jsonwebtoken');
const CUBE_API_SECRET='secret';

const cubejsToken = jwt.sign({ u: { id: 42 } }, CUBEJS_API_SECRET, { expiresIn: '30d' });
```

Using this token we can sign our request to Cube.js Backend.

```bash
curl \
 -H "Authorization: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1Ijp7ImlkIjo0Mn0sImlhdCI6MTU1NjAyNTM1MiwiZXhwIjoxNTU4NjE3MzUyfQ._8QBL6nip6SkIrFzZzGq2nSF8URhl5BSSSGZYp7IJZ4" \
 -G \
 --data-urlencode 'query={"measures":["Orders.count"]}' \
 http://localhost:4000/cubejs-api/v1/load
````

And the following SQL will be generated by Cube.js.

```sql
SELECT
  count(*) "orders.count"
  FROM (
    SELECT * FROM public.orders WHERE user_id = 42
  ) AS orders
LIMIT 10000
```

## Enabling TLS

Cube.js server package supports transport layer encryption.

By setting the environment variable `CUBEJS_ENABLE_TLS` to true (`CUBEJS_ENABLE_TLS=true`), `@cubejs-backend/server` expects an argument to its `listen` function specifying the tls encryption options. The `tlsOption` object must match Node.js' [https.createServer([options][, requestListener])](https://nodejs.org/api/https.html#https_https_createserver_options_requestlistener) option object.

This enables you to specify your TLS security directly within the Node process without having to rely on external deployment tools to manage your certificates.

```javascript
const fs = require("fs-extra");
const CubejsServer = require("@cubejs-backend/server");

var tlsOptions = {
  key: fs.readFileSync(process.env.CUBEJS_TLS_PRIVATE_KEY_FILE),
  cert: fs.readFileSync(process.env.CUBEJS_TLS_PRIVATE_FULLCHAIN_FILE),
};

const cubejsServer = new CubejsServer();

cubejsServer.listen(tlsOptions).then(({ version, tlsPort }) => {
  console.log(`🚀 Cube.js server (${version}) is listening securely on ${tlsPort}`);
});
```

Notice that the response from the resolution of `listen`'s promise returns more than just the `port` and the express `app` as it would normally do without `CUBEJS_ENABLE_TLS` enabled. When `CUBEJS_ENABLE_TLS` is enabled, `cubejsServer.listen` will resolve with the following:

* `port {number}` The port at which CubejsServer is listening for insecure connections for redirection to HTTPS, as specified by the environment variable `PORT`. Defaults to 4000.
* `tlsPort {number}` The port at which TLS is enabled, as specified by the environment variable `TLS_PORT`. Defaults to 4433.
* `app {Express.Application}` The express App powering CubejsServer
* `server {https.Server}` The `https` Server instance.

The `server` object is especially useful if you want to use self-signed, self-renewed certificates.

### Self-signed, self-renewed certificates

Self-signed, self-renewed certificates are useful when dealing with internal data transit, like when answering requests from private server instance to another private server instance without being able to use an external DNS CA to sign the private certificates. _Example:_ EC2 to EC2 instance communications within the private subnet of a VPC.

Here is an example of how to do leverage `server` to have self-signed, self-renewed encryption:

```js
const CubejsServer = require("@cubejs-backend/server");

const {
  createCertificate,
  scheduleCertificateRenewal,
} = require("./certificate");

async function main() {
  const cubejsServer = new CubejsServer();

  const certOptions = { days: 2, selfSigned: true };
  const tlsOptions = await createCertificate(certOptions);

  const ({ version, tlsPort, server }) = await cubejsServer.listen(tlsOptions);

  console.log(`🚀 Cube.js server (${version}) is listening securely on ${tlsPort}`);

  scheduleCertificateRenewal(server, certOptions, (err, result) => {
    if (err !== null) {
      console.error(
        `🚨 Certificate renewal failed with error "${error.message}"`
      );
      // take some action here to notify the DevOps
      return;
    }
    console.log(`🔐 Certificate renewal successful`);
  });
}

main();
```

To generate your self-signed certificates, look into [pem](https://www.npmjs.com/package/pem) and [node-forge](https://www.npmjs.com/package/node-forge).

### Node Support for Self Renewal of Secure Context

Certificate Renewal using [server.setSecureContext(options)](https://nodejs.org/api/tls.html#tls_server_setsecurecontext_options) is only available as of Node.js v11.x
