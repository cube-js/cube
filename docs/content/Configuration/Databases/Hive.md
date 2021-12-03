---
title: Hive/SparkSQL
permalink: /config/databases/hive-sparksql
---

## Prerequisites

- The hostname for the [Hive][hive] database server
- The username/password for the [Hive][hive] database server

## Setup

### <--{"id" : "Setup"}--> Manual

Add the following to a `.env` file in your Cube.js project:

```bash
CUBEJS_DB_TYPE=hive
CUBEJS_DB_HOST=my.hive.host
CUBEJS_DB_NAME=my_hive_database
CUBEJS_DB_USER=hive_user
CUBEJS_DB_PASS=**********
```

## Environment Variables

| Environment Variable | Description                                  | Possible Values           | Required |
| -------------------- | -------------------------------------------- | ------------------------- | :------: |
| `CUBEJS_DB_HOST`     | The host URL for a database                  | A valid database host URL |    ✅    |
| `CUBEJS_DB_PORT`     | The port for the database connection         | A valid port number       |    ❌    |
| `CUBEJS_DB_NAME`     | The name of the database to connect to       | A valid database name     |    ✅    |
| `CUBEJS_DB_USER`     | The username used to connect to the database | A valid database username |    ✅    |
| `CUBEJS_DB_PASS`     | The password used to connect to the database | A valid database password |    ✅    |

[hive]: https://hive.apache.org/
