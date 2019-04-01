---
title: Security
permalink: /security
category: Cube.js Backend
---

Cube.js uses [JSON Web Tokens (JWT)](https://jwt.io/) as an auth token to access
an API.

Cube.js Javascript client accepts auth token as a first argument to [cubejs(authToken, options) function](@cubejs-client-core#cubejs).
Auth token is generated based on your API secret. Cube.js CLI generates API Secret on app creation and saves it in `.env` file as `CUBEJS_API_SECRET` variable.

You can find a library for JWT generation [here](https://jwt.io/#libraries-io) or generate it manually on [JWT tokens site](https://jwt.io/) for testing purpose.

You can generate two types of tokens:
- Without security context. It implies same data access permissions for all users.
- With security context. User or role-based security models can be implemented using this approach.

Security context can be provided by passing `u` param for payload.
For example if you want to pass user id in security context you can create token with payload:
```json
{
  "u": { "id": 42 }
}
```

In this case `{ "id": 42 }` object will be accessible as `USER_CONTEXT` in cube.js Data Schema.
[Learn more here](cube#context-variables-user-context).

> *NOTE*: We strongly encourage you to use `exp` expiration claim to limit life time of your public tokens.
> Learn more: [JWT docs](https://github.com/auth0/node-jsonwebtoken#token-expiration-exp-claim).
