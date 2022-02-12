---
title: Oracle
permalink: /config/databases/oracle
---

## Prerequisites

- The hostname for the [Oracle][oracle] database server
- The username/password for the [Oracle][oracle] database server
- The name of the database to use within the [Oracle][oracle] database server

## Setup

### <--{"id" : "Setup"}--> Manual

Add the following to a `.env` file in your Cube.js project:

```bash
CUBEJS_DB_TYPE=oracle
CUBEJS_DB_HOST=my.oracle.host
CUBEJS_DB_NAME=my_oracle_database
CUBEJS_DB_USER=oracle_user
CUBEJS_DB_PASS=**********
```

## Environment Variables

| Environment Variable | Description                                                             | Possible Values           | Required |
| -------------------- | ----------------------------------------------------------------------- | ------------------------- | :------: |
| `CUBEJS_DB_HOST`     | The host URL for a database                                             | A valid database host URL |    ✅    |
| `CUBEJS_DB_PORT`     | The port for the database connection                                    | A valid port number       |    ❌    |
| `CUBEJS_DB_NAME`     | The name of the database to connect to                                  | A valid database name     |    ✅    |
| `CUBEJS_DB_USER`     | The username used to connect to the database                            | A valid database username |    ✅    |
| `CUBEJS_DB_PASS`     | The password used to connect to the database                            | A valid database password |    ✅    |
| `CUBEJS_DB_SSL`      | If `true`, enables SSL encryption for database connections from Cube.js | `true`, `false`           |    ❌    |

## SSL

To enable SSL-encrypted connections between Cube.js and Oracle, set the
`CUBEJS_DB_SSL` environment variable to `true`. For more information on how to
configure custom certificates, please check out [Enable SSL Connections to the
Database][ref-recipe-enable-ssl].

[oracle]: https://www.oracle.com/uk/index.html
[ref-recipe-enable-ssl]: /recipes/enable-ssl-connections-to-database
