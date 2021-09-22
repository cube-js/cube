---
title: PrestoDB
permalink: /config/databases/prestodb
---

## Prerequisites

- The hostname for the [PrestoDB][prestodb] database server
- The username/password for the [PrestoDB][prestodb] database server
- The name of the database to use within the [PrestoDB][prestodb] database
  server

## Setup

### <--{"id" : "Setup"}--> Manual

Add the following to a `.env` file in your Cube.js project:

```bash
CUBEJS_DB_TYPE=prestodb
CUBEJS_DB_HOST=my.prestodb.host
CUBEJS_DB_NAME=my_prestodb_database
CUBEJS_DB_USER=prestodb_user
CUBEJS_DB_PASS=**********
CUBEJS_DB_CATALOG=my_prestodb_catalog
CUBEJS_DB_SCHEMA=my_prestodb_schema
```

## Environment Variables

| Environment Variable | Description                                                             | Possible Values                               | Required |
| -------------------- | ----------------------------------------------------------------------- | --------------------------------------------- | :------: |
| `CUBEJS_DB_HOST`     | The host URL for a database                                             | A valid database host URL                     |    ✅    |
| `CUBEJS_DB_PORT`     | The port for the database connection                                    | A valid port number                           |    ❌    |
| `CUBEJS_DB_USER`     | The username used to connect to the database                            | A valid database username                     |    ✅    |
| `CUBEJS_DB_PASS`     | The password used to connect to the database                            | A valid database password                     |    ✅    |
| `CUBEJS_DB_CATALOG`  | The catalog within the database to connect to                           | A valid catalog name within a Presto database |    ✅    |
| `CUBEJS_DB_SCHEMA`   | The schema within the database to connect to                            | A valid schema name within a Presto database  |    ✅    |
| `CUBEJS_DB_SSL`      | If `true`, enables SSL encryption for database connections from Cube.js | `true`, `false`                               |    ❌    |

## SSL

To enable SSL-encrypted connections between Cube.js and PrestoDB, set the
`CUBEJS_DB_SSL` environment variable to `true`. For more information on how to
configure custom certificates, please check out [Enable SSL Connections to the
Database][ref-recipe-enable-ssl].

[prestodb]: https://prestodb.io/
[ref-recipe-enable-ssl]: /recipes/enable-ssl-connections-to-database
