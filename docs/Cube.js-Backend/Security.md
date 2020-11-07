---
title: Security
permalink: /security
category: Cube.js Backend
menuOrder: 8
---

Cube.js uses [JSON Web Tokens (JWT)](https://jwt.io/) which should be passed in the `Authorization` header to authenticate requests. JWTs can also be used for passing
additional information about the user, which can be accessed in the [USER_CONTEXT](cube#context-variables-user-context) object in the Data
Schema. 

The `Authorization` header is parsed and the JWT's contents set to the [authInfo](@cubejs-backend-server-core#authinfo) variable which can be used to support [Multitenancy](multitenancy-setup).

Cube.js tokens are designed to work well in microservice-based environments. Typical use case would be:

1. A web server serves an HTML page containing the cube.js client, which needs to communicate securely with the cube.js back-end.
2. The web server should generate an expirable cube.js token to achieve this. The server could include the token in the HTML it serves or provide the token to the front-end via an XHR request, which stores it in local storage or a cookie.
3. The JS client is initialized using this token, and includes it in calls to the cube.js server API.

If you are using the [REST API](rest-api) you must pass the API Token via the Authorization Header. The Cube.js Javascript client accepts an authentication token as the first argument to the [cubejs(authToken, options) function](@cubejs-client-core#cubejs).

**In the development environment the token is not required for authorization**, but
you can still use it to [pass a security context](security#security-context).

Cube.js also supports Transport Layer Encryption (TLS) using Node.js native packages. For more information, see [Enabling TLS](security#enabling-tls).

## Generating Tokens

Authentication tokens are generated based on your API secret. Cube.js CLI generates an API Secret when the app is first created and saves this value in the `.env` file as `CUBEJS_API_SECRET`.

You can generate two types of tokens:
- Without security context, which will mean that all users will have the same data access permissions.
- With security context, which will allow you to implement role-based security models where users will have different levels of access to data.

_It is considered best practice to use an `exp` expiration claim to limit the life time of your public tokens.
[Learn more at JWT docs](https://github.com/auth0/node-jsonwebtoken#token-expiration-exp-claim)._

### Using the CLI

You can use the Cube.js CLI [token](reference#token) command to generate an API token.

```bash
$ cubejs token -e TOKEN-EXPIRY -s SECRET -p FOO=BAR
```

It is helpful to be able to create an API token with the CLI command for testing and development purposes, but we strongly recommend to programmatically generate tokens in production.

### Programmatically

You can find a library for JWT generation for your programming language [here](https://jwt.io/#libraries-io).

In Node.js, the following code shows how to generate a token which will expire in 30 days. We recommend using the `jsonwebtoken` package for this.

```javascript
const jwt = require('jsonwebtoken');
const CUBE_API_SECRET='secret';

const cubejsToken = jwt.sign({}, CUBE_API_SECRET, { expiresIn: '30d' });
```

Then, in a web server or cloud function, create a route which generates and returns a token. In general, you will want to protect the URL that generates your token using your own user authentication and authorization:

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

Then, on the client side, (assuming the user is signed in), fetch a token from the web server:

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

You can optionally store this token in local storage or in a cookie, so that you can then use it to query the cube.js API.

## Security Context

A "security context" is a verified set of claims about the current user that the cube.js server can use to ensure that users only have access to the data that they are authorized to access. You can provide a security context by passing the `u` param in the JSON payload that you pass to your JWT signing function.
For example if you want to pass the user ID in the security context you could create a token with this json structure:

```json
{
  "u": { "id": 42 }
}
```

In this case, the `{ "id": 42 }` object will be accessible as [USER_CONTEXT](cube#context-variables-user-context) in the Cube.js Data Schema.

The Cube.js server expects the context to be an object. If you don't provide an object as the JWT payload, you will receive an error of the form `Cannot create proxy with a non-object as target or handler`.

Consider the following example. We want to show orders only for customers who own these orders. The `orders` table has a `user_id` column, which we
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

Now, we can generate an API Token with a security context including the user's ID:

```javascript
const jwt = require('jsonwebtoken');
const CUBE_API_SECRET='secret';

const cubejsToken = jwt.sign({ u: { id: 42 } }, CUBEJS_API_SECRET, { expiresIn: '30d' });
```

Using this token, we authorize our request to the Cube.js API by passing it in the Authorization HTTP header.

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

The Cube.js server package supports transport layer encryption (TLS).

By setting the environment variable `CUBEJS_ENABLE_TLS` to true (`CUBEJS_ENABLE_TLS=true`), `@cubejs-backend/server` expects an argument to its `listen` function specifying the TLS encryption options. The `tlsOption` object must match Node.js's [https.createServer([options][, requestListener])](https://nodejs.org/api/https.html#https_https_createserver_options_requestlistener) `option` object.

This enables you to specify your TLS security directly within the Node process, without having to rely on external deployment tools to manage your certificates.

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

* `port {number}` The port at which the Cubejs Server is listening for insecure connections for redirection to HTTPS, as specified by the environment variable `PORT`. Defaults to 4000.
* `tlsPort {number}` The port at which TLS is enabled, as specified by the environment variable `TLS_PORT`. Defaults to 4433.
* `app {Express.Application}` The express App powering the Cubejs Server
* `server {https.Server}` The `https` Server instance.

The `server` object is especially useful if you want to use self-signed, self-renewed certificates.

### Self-signed, self-renewed certificates

Self-signed, self-renewed certificates are useful when dealing with internal data transit, like when answering requests from one private server instance to another private server instance, without being able to use an external DNS CA to sign the private certificates. _Example:_ EC2 to EC2 instance communications within the private subnet of a VPC.

Here is an example of how to use `server` to support self-signed, self-renewed encryption:

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
