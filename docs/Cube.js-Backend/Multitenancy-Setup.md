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

Now, we can access them as `authInfo` object inside the context object. Let's
first use `contextToAppId` to create a dynamic Cube.js App ID for every combination of
`appId` and `userId`. Cube.js App ID is used as caching key for various in-memory structures like schema compilation results, connection pool, etc.


```javascript
new CubejsServer({
  contextToAppId: ({ authInfo }) => `CUBEJS_APP_${authInfo.appId}_${authInfo.userId}`
})
```

Next, we can use `driverFactory` to dynamically select database, based on
`appId` and `userId`.

```javascript
const PostgresDriver = require("@cubejs-backend/postgres-driver");

new CubejsServer({
  contextToAppId: ({ authInfo }) => `CUBEJS_APP_${authInfo.appId}_${authInfo.userId}`,
  driverFactory: ({ authInfo }) =>
    new PostgresDriver({
      database: `my_app_${authInfo.appId}_${authInfo.userId}`
    })
});
```

What if for application with ID 3 data is stored not in Postgres, but in
MongoDB? We can instruct Cube.js to connect to MongoDB in that case, instead of
Postgres. For that purpose we'll use `dbType` option to dynamically set database
type. We also need to modify our `driverFactory` option.

```javascript
const PostgresDriver = require("@cubejs-backend/postgres-driver");
const MongoBIDriver = require('@cubejs-backend/mongobi-driver');

new CubejsServer({
  contextToAppId: ({ authInfo }) => `CUBEJS_APP_${authInfo.appId}_${authInfo.userId}`,
  dbType: ({ authInfo }) => {
    if (authInfo.appId === 3) {
      return 'mongobi';
    } else {
      return 'postgres';
    }
  },
  driverFactory: ({ authInfo }) => {
    if (authInfo.appId === 3) {
      return new MongoBIDriver({
        database: `my_app_${authInfo.appId}_${authInfo.userId}`
        port: 3307
      })
    } else {
      return new PostgresDriver({
        database: `my_app_${authInfo.appId}_${authInfo.userId}`
      })
    }
  }
});
```

Lastly, we want to have separate data schemas for every application. In this case we can
use `repositoryFactory` option to dynamically set a repository with schema files depending on the `appId`.

Below you can find final setup with `repositoryFactory` option.

```javascript
const PostgresDriver = require("@cubejs-backend/postgres-driver");
const MongoBIDriver = require('@cubejs-backend/mongobi-driver');
const FileRepository = require('@cubejs-backend/server-core/core/FileRepository');

new CubejsServer({
  contextToAppId: ({ authInfo }) => `CUBEJS_APP_${authInfo.appId}_${authInfo.userId}`,
  dbType: ({ authInfo }) => {
    if (authInfo.appId === 3) {
      return 'mongobi';
    } else {
      return 'postgres';
    }
  },
  driverFactory: ({ authInfo }) => {
    if (authInfo.appId === 3) {
      return new MongoBIDriver({
        database: `my_app_${authInfo.appId}_${authInfo.userId}`
        port: 3307
      })
    } else {
      return new PostgresDriver({
        database: `my_app_${authInfo.appId}_${authInfo.userId}`
      })
    }
  },
  repositoryFactory: ({ authInfo }) => new FileRepository(`schema/${authInfo.appId}`)
});
```
