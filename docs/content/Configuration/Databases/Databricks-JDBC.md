---
title: Databricks JDBC
permalink: /config/databases/jdbc/databricks
---

## Prerequisites

- [A JDK installation][gh-cubejs-jdbc-install]
- The [JDBC URL][databricks-docs-jdbc-url] for the [Databricks][databricks]
  cluster

## Setup

### <--{"id" : "Setup"}--> Manual

Add the following to a `.env` file in your Cube.js project:

```dotenv
CUBEJS_DB_TYPE=databricks-jdbc
# CUBEJS_DB_NAME is optional
CUBEJS_DB_NAME=default
# You can find this inside the cluster's configuration
CUBEJS_DB_DATABRICKS_URL=jdbc:spark://dbc-XXXXXXX-XXXX.cloud.databricks.com:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/XXXXX/XXXXX;AuthMech=3;UID=token;PWD=XXXXX
```

## Environment Variables

| Environment Variable       | Description                            | Possible Values       | Required |
| -------------------------- | -------------------------------------- | --------------------- | :------: |
| `CUBEJS_DB_NAME`           | The name of the database to connect to | A valid database name |    ✅    |
| `CUBEJS_DB_DATABRICKS_URL` | The URL for a JDBC connection          | A valid JDBC URL      |    ✅    |

[databricks]: https://databricks.com/
[databricks-docs-jdbc-url]:
  https://docs.databricks.com/integrations/bi/jdbc-odbc-bi.html#get-server-hostname-port-http-path-and-jdbc-url
[gh-cubejs-jdbc-install]:
  https://github.com/cube-js/cube.js/blob/master/packages/cubejs-jdbc-driver/README.md#java-installation
