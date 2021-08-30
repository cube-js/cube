---
title: MySQL
permalink: /config/databases/mysql
---

## Prerequisites

- The hostname for the [MySQL][mysql] database server
- The username/password for the [MySQL][mysql] database server
- The name of the database to use within the [MySQL][mysql] database server

## Setup

### Manual

Add the following to a `.env` file in your Cube.js project:

```bash
CUBEJS_DB_TYPE=mysql
CUBEJS_DB_HOST=my.mysql.host
CUBEJS_DB_NAME=my_mysql_database
CUBEJS_DB_USER=mysql_user
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

To enable SSL-encrypted connections between Cube.js and MySQL, set the
`CUBEJS_DB_SSL` environment variable to `true`. For more information on how to
configure custom certificates, please check out [Enable SSL Connections to the
Database][ref-recipe-enable-ssl].

## Additional Configuration

### Local/Docker

To connect to a local MySQL database using a Unix socket, use
`CUBEJS_DB_SOCKET_PATH`. When doing so, `CUBEJS_DB_HOST` will be ignored.

You can connect to an SSL-enabled MySQL database by setting `CUBEJS_DB_SSL` to
`true`. All other SSL-related environment variables can be left unset. See [the
SSL section][self-ssl] above for more details.

[mysql]: https://www.mysql.com/
[ref-recipe-enable-ssl]: /recipes/enable-ssl-connections-to-database
[self-ssl]: #ssl
