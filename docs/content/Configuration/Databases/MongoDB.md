---
title: MongoDB
permalink: /config/databases/mongodb
---

## Prerequisites

<!-- prettier-ignore-start -->
[[info |]]
| To use Cube.js with MongoDB you need to install the [MongoDB Connector for
| BI][mongobi-download]. [Learn more about setup for MongoDB
| here][cube-blog-mongodb].
<!-- prettier-ignore-end -->

- [MongoDB Connector for BI][mongobi-download]
- The hostname for the [MongoDB][mongodb] database server
- The username/password for the [MongoDB][mongodb] database server

## Setup

### Manual

Add the following to a `.env` file in your Cube.js project:

```bash
CUBEJS_DB_TYPE=mongobi
CUBEJS_DB_HOST=my.mongobi.host
CUBEJS_DB_NAME=my_mongodb_database
CUBEJS_DB_USER=mongobi_user
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

To enable SSL-encrypted connections between Cube.js and MongoDB, set the
`CUBEJS_DB_SSL` environment variable to `true`. For more information on how to
configure custom certificates, please check out [Enable SSL Connections to the
Database][ref-recipe-enable-ssl].

## Additional Configuration

### MongoDB Atlas

Use `CUBEJS_DB_SSL=true` to enable SSL as MongoDB Atlas requires it. All other
SSL-related environment variables can be left unset.

[mongodb]: https://www.mongodb.com/
[cube-blog-mongodb]:
  https://cube.dev/blog/building-mongodb-dashboard-using-node.js
[mongobi-download]: https://www.mongodb.com/download-center/bi-connector
[nodejs-docs-tls-ciphers]:
  https://nodejs.org/docs/latest/api/tls.html#tls_modifying_the_default_tls_cipher_suite
[ref-recipe-enable-ssl]: /recipes/enable-ssl-connections-to-database
