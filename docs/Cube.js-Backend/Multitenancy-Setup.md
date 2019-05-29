---
title: Multitenancy Setup
permalink: /multitenancy-setup
category: Cube.js Backend
menuOrder: 5
---

Cube.js supports multitenancy out of the box, both on database and data schema levels. Multiple drivers are also supported, meaning that you can have one customer’s data in MongoDB and others in Postgres with one Cube.js instance.

There are 4 configuration options you can leverage to make your multitenancy setup. You
can use all of them or just a couple, depending on your specific case. The
options are:

- `contextToAppId`
- `dbType`
- `driverFactory`
- `repositoryFactory`

All of the above options are functions, which you provide on Cube.js server instance creation. The
functions accept one argument - context object, which has a nested object -
`authInfo`, which acts as a container, where you can provide all the necessary data to identify user, organization, app, etc. You put data into `authInfo` when creating a Cube.js API Token.

## Example

Let's consider the following example:

We store data for different users in different databases, but on the same Postgres host. The database name is `my_app_1_2`, where `1`
is **Application ID** and `2` is **User ID**.

To make it work with Cube.js,
first we need to pass the `appId` and `userId` as context to every query. We
should include that into our token generation code.

```javascript
const jwt = require('jsonwebtoken');
const CUBE_API_SECRET='secret';

const cubejsToken = jwt.sign(
  { appId: appId, userId: userId },
  CUBE_API_SECRET,
  { expiresIn: '30d' }
);
```

Now, we can access them inside the `driverFactory` option.

```javascript
const PostgresDriver = require("@cubejs-backend/postgres-driver");

new CubejsServer({
  driverFactory: ({ authInfo }) =>
    new PostgresDriver({
      database: `my_app_${authInfo.appId}_${authInfo.userId}`
    })
});
```
