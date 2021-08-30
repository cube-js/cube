---
title: ClickHouse
permalink: /config/databases/clickhouse
---

## Prerequisites

- The hostname for the [ClickHouse][clickhouse] database server
- The [username/password][clickhouse-docs-users] for the
  [ClickHouse][clickhouse] database server

## Setup

### Manual

Add the following to a `.env` file in your Cube.js project:

```bash
CUBEJS_DB_TYPE=clickhouse
CUBEJS_DB_HOST=my.clickhouse.host
CUBEJS_DB_NAME=my_clickhouse_database
CUBEJS_DB_USER=clickhouse_user
CUBEJS_DB_PASS=**********
```

## Environment Variables

| Environment Variable            | Description                                             | Possible Values           | Required |
| ------------------------------- | ------------------------------------------------------- | ------------------------- | :------: |
| `CUBEJS_DB_HOST`                | The host URL for a database                             | A valid database host URL |    ✅    |
| `CUBEJS_DB_PORT`                | The port for the database connection                    | A valid port number       |    ❌    |
| `CUBEJS_DB_NAME`                | The name of the database to connect to                  | A valid database name     |    ✅    |
| `CUBEJS_DB_USER`                | The username used to connect to the database            | A valid database username |    ✅    |
| `CUBEJS_DB_PASS`                | The password used to connect to the database            | A valid database password |    ✅    |
| `CUBEJS_DB_CLICKHOUSE_READONLY` | Whether the ClickHouse user has read-only access or not | `true`, `false`           |    ❌    |

## SSL

To enable SSL-encrypted connections between Cube.js and ClickHouse, set the
`CUBEJS_DB_SSL` environment variable to `true`. For more information on how to
configure custom certificates, please check out [Enable SSL Connections to the
Database][ref-recipe-enable-ssl].

## Additional Configuration

You can connect to a ClickHouse database when your user's permissions are
[restricted][clickhouse-readonly] to read-only, by setting
`CUBEJS_DB_CLICKHOUSE_READONLY` to `true`.

[clickhouse]: https://clickhouse.tech/
[clickhouse-docs-users]:
  https://clickhouse.tech/docs/en/operations/settings/settings-users/
[clickhouse-readonly]:
  https://clickhouse.tech/docs/en/operations/settings/permissions-for-queries/#settings_readonly
[ref-recipe-enable-ssl]: /recipes/enable-ssl-connections-to-database
