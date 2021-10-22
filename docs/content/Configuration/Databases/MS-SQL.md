---
title: MS SQL
permalink: /config/databases/mssql
---

## Prerequisites

- The hostname for the [MS SQL][mssql] database server
- The username/password for the [MS SQL][mssql] database server
- The name of the database to use within the [MS SQL][mssql] database server

## Setup

### <--{"id" : "Setup"}-->  Manual

Add the following to a `.env` file in your Cube.js project:

```bash
CUBEJS_DB_TYPE=mssql
CUBEJS_DB_HOST=my.mssql.host
CUBEJS_DB_NAME=my_mssql_database
CUBEJS_DB_USER=mssql_user
CUBEJS_DB_PASS=**********
```

## Environment Variables

| Environment Variable | Description                                                             | Possible Values                             | Required |
| -------------------- | ----------------------------------------------------------------------- | ------------------------------------------- | :------: |
| `CUBEJS_DB_HOST`     | The host URL for a database                                             | A valid database host URL                   |    ✅    |
| `CUBEJS_DB_PORT`     | The port for the database connection                                    | A valid port number                         |    ❌    |
| `CUBEJS_DB_NAME`     | The name of the database to connect to                                  | A valid database name                       |    ✅    |
| `CUBEJS_DB_USER`     | The username used to connect to the database                            | A valid database username                   |    ✅    |
| `CUBEJS_DB_PASS`     | The password used to connect to the database                            | A valid database password                   |    ✅    |
| `CUBEJS_DB_DOMAIN`   | A domain name within the database to connect to                         | A valid domain name within a MSSQL database |    ❌    |
| `CUBEJS_DB_SSL`      | If `true`, enables SSL encryption for database connections from Cube.js | `true`, `false`                             |    ❌    |

## SSL

To enable SSL-encrypted connections between Cube.js and MS SQL, set the
`CUBEJS_DB_SSL` environment variable to `true`. For more information on how to
configure custom certificates, please check out [Enable SSL Connections to the
Database][ref-recipe-enable-ssl].

## Additional Configuration

### <--{"id" : "Additional Configuration"}-->  Windows Authentication

To connect to a MSSQL database using Windows Authentication (also sometimes
known as `trustedConnection`), instantiate the driver with
`trustedConnection: true` in your `cube.js` configuration file:

```javascript
const MssqlDriver = require('@cubejs-backend/mssql-driver');
module.exports = {
  driverFactory: ({ dataSource }) =>
    new MssqlDriver({ database: dataSource, trustedConnection: true }),
};
```

[mssql]: https://www.microsoft.com/en-gb/sql-server/sql-server-2019
[ref-recipe-enable-ssl]: /recipes/enable-ssl-connections-to-database
