---
title: Generating JWTs for Web Apps
permalink: /recipes/generating-jwts-for-web-apps
category: Examples & Tutorials
subCategory: Access control
menuOrder: 10
---

By default, Cube uses [JSON Web Tokens][jwt] to authenticate requests to the REST and GraphQL APIs.
This recipe will show you how to generate a JWT for a user in your web app.

When you scaffold a new project via the Cube CLI, an API Secret is automatically created and saved in the
`.env` file as `CUBEJS_API_SECRET`.

You can generate two types of tokens:

- Without security context, which will mean that all users will have the same
  data access permissions.
- With security context, which will allow you to implement role-based security
  models where users will have different levels of access to data.

<InfoBox>

It is considered best practice to use an `exp` expiration claim to limit the
lifetime of your public tokens. [Learn more in the `jsonwebtoken` docs][jsonwebtoken-docs].

</InfoBox>

You can find a library to generate JWTs for your programming language
[here][jwt-libs].

In Node.js, the following code shows how to generate a token which will expire
in 30 days. We recommend using the [`jsonwebtoken`][npm-jsonwebtoken] package for this.

```javascript
const jwt = require('jsonwebtoken');
const CUBE_API_SECRET = 'secret';

const cubejsToken = jwt.sign({}, CUBE_API_SECRET, { expiresIn: '30d' });
```

Then, in a web server or cloud function, create a route which generates and
returns a token. In general, you will want to protect the URL that generates
your token using your own user authentication and authorization. The following example shows
how to do this for an Express server:

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
    token: jwt.sign(req.user, process.env.CUBEJS_API_SECRET, {
      expiresIn: '1d',
    }),
  });
});
```

Then, on the client side, (assuming the user is already signed in), fetch a token from
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
can then use it to query the Cube REST or GraphQL API.

[jwt]: https://jwt.io/
[jsonwebtoken-docs]:
  https://github.com/auth0/node-jsonwebtoken#token-expiration-exp-claim
[jwt-libs]: https://jwt.io/#libraries-io
[npm-jsonwebtoken]: https://www.npmjs.com/package/jsonwebtoken
