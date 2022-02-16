---
title: SQLite
permalink: /config/databases/sqlite
---

## Prerequisites

## Setup

### <--{"id" : "Setup"}--> Manual

Add the following to a `.env` file in your Cube.js project:

```bash
CUBEJS_DB_TYPE=sqlite
CUBEJS_DB_NAME=my_sqlite_database
```

## Environment Variables

| Environment Variable | Description                            | Possible Values       | Required |
| -------------------- | -------------------------------------- | --------------------- | :------: |
| `CUBEJS_DB_NAME`     | The name of the database to connect to | A valid database name |    ✅    |

## SSL

SQLite does not support SSL connections.
