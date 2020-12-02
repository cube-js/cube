---
title: Connecting to the Database
permalink: /connecting-to-the-database
category: Configuration
menuOrder: 2
---

Cube.js currently provides connectors to the following databases:

| Database                               | Cube.js DB Type |
| -------------------------------------- | --------------- |
| PostgreSQL                             | `postgres`      |
| MySQL                                  | `mysql`         |
| AWS Athena                             | `athena`        |
| AWS Redshift                           | `redshift`      |
| MongoDB (via MongoDB Connector for BI) | `mongobi`       |
| Google BigQuery                        | `bigquery`      |
| MS SQL                                 | `mssql`         |
| ClickHouse                             | `clickhouse`    |
| Snowflake                              | `snowflake`     |
| Presto                                 | `prestodb`      |
| Hive / SparkSQL (thrift)               | `hive`          |
| Oracle                                 | `oracle`        |
| Apache Druid                           | `druid`         |
| SQLite                                 | `sqlite`        |

<!-- prettier-ignore-start -->
[[info | ]]
| If you'd like to connect to a database which is not yet supported,
| you can create a Cube.js-compliant driver package.
| [Here's a simple step-by-step guide][link-cubejs-driver-guide].
<!-- prettier-ignore-end -->

[link-cubejs-driver-guide]:
  https://github.com/cube-js/cube.js/blob/master/CONTRIBUTING.md#implementing-driver

## Source Database

When you create a new Cube.js app with the [Cube.js CLI][link-cubejs-cli], the
`.env` will be generated to manage all connection credentials. The set of
variables could be different based on your database type. For example, for
PostgreSQL the `.env` will look like this:

[link-cubejs-cli]: /using-the-cubejs-cli

```bash
CUBEJS_DB_HOST=<YOUR_DB_HOST_HERE>
CUBEJS_DB_NAME=<YOUR_DB_NAME_HERE>
CUBEJS_DB_USER=<YOUR_DB_USER_HERE>
CUBEJS_DB_PASS=<YOUR_DB_PASS_HERE>
CUBEJS_DB_TYPE=postgres
CUBEJS_API_SECRET=secret
```

The table below shows which environment variables are used for different
databases:

| Database                                                           | Credentials                                                                                                                                                                                                                     |
| ------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| PostgreSQL, MySQL, AWS Redshift, ClickHouse, Hive/SparkSQL, Oracle | `CUBEJS_DB_HOST`, `CUBEJS_DB_PORT`, `CUBEJS_DB_NAME`, `CUBEJS_DB_USER`, `CUBEJS_DB_PASS`                                                                                                                                        |
| MS SQL                                                             | `CUBEJS_DB_HOST`, `CUBEJS_DB_PORT`, `CUBEJS_DB_NAME`, `CUBEJS_DB_USER`, `CUBEJS_DB_PASS`, `CUBEJS_DB_DOMAIN`                                                                                                                    |
| AWS Athena                                                         | `CUBEJS_AWS_KEY`, `CUBEJS_AWS_SECRET`, `CUBEJS_AWS_REGION`, `CUBEJS_AWS_S3_OUTPUT_LOCATION`                                                                                                                                     |
| Google Bigquery                                                    | `CUBEJS_DB_BQ_PROJECT_ID`, `CUBEJS_DB_BQ_KEY_FILE or CUBEJS_DB_BQ_CREDENTIALS`                                                                                                                                                  |
| MongoDB                                                            | `CUBEJS_DB_HOST`, `CUBEJS_DB_NAME`, `CUBEJS_DB_PORT`, `CUBEJS_DB_USER`, `CUBEJS_DB_PASS`, `CUBEJS_DB_SSL`, `CUBEJS_DB_SSL_CA`, `CUBEJS_DB_SSL_CERT`, `CUBEJS_DB_SSL_CIPHERS`, `CUBEJS_DB_SSL_PASSPHRASE`                        |
| Snowflake                                                          | `CUBEJS_DB_SNOWFLAKE_ACCOUNT`, `CUBEJS_DB_SNOWFLAKE_REGION`, `CUBEJS_DB_SNOWFLAKE_WAREHOUSE`, `CUBEJS_DB_SNOWFLAKE_ROLE`, `CUBEJS_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE`, `CUBEJS_DB_NAME`, `CUBEJS_DB_USER`, `CUBEJS_DB_PASS` |
| Presto                                                             | `CUBEJS_DB_HOST`, `CUBEJS_DB_PORT`, `CUBEJS_DB_CATALOG`, `CUBEJS_DB_SCHEMA`, `CUBEJS_DB_USER`, `CUBEJS_DB_PASS`                                                                                                                 |
| Druid                                                              | `CUBEJS_DB_URL`, `CUBEJS_DB_USER`, `CUBEJS_DB_PASS`                                                                                                                                                                             |
| SQLite                                                             | `CUBEJS_DB_NAME`                                                                                                                                                                                                                |

## External Pre-aggregations Database

To enable [external pre-aggregations][link-external-preaggregation] you need to
configure an external database to store these pre-aggregations.

[link-external-preaggregation]: pre-aggregations#external-pre-aggregations

Cube.js provides a set of environment variables to configure a connection to an
external database:

```bash
CUBEJS_EXT_DB_HOST=<YOUR_DB_HOST_HERE>
CUBEJS_EXT_DB_PORT=<YOUR_DB_PORT_HERE>
CUBEJS_EXT_DB_NAME=<YOUR_DB_NAME_HERE>
CUBEJS_EXT_DB_USER=<YOUR_DB_USER_HERE>
CUBEJS_EXT_DB_PASS=<YOUR_DB_PASS_HERE>
CUBEJS_EXT_DB_TYPE=<SUPPORTED_DB_TYPE_HERE>
```

## Enabling SSL

Cube.js supports SSL-encrypted connections for **Postgres**, **MongoDB** and
**MySQL**. To enable it set the `CUBEJS_DB_SSL` environment variable to `true`.
Cube.js can also be configured to use custom connection settings. For example,
to use a custom CA, you could do the following:

```dotenv
CUBEJS_DB_SSL_CA="-----BEGIN CERTIFICATE-----\nMIIEBjCCAu6gAwIBAgIJAMc0ZzaSUK51MA0GCSqGSIb3DQEBCwUAMIGPMQswCQYD\nVQQGEwJVUzEQMA4GA1UEBwwHU2VhdHRsZTETMBEGA1UECAwKV2FzaGluZ3RvbjEi\n-----END CERTIFICATE-----"
```

For a complete list of SSL-related environment variables, consult the [Database
Connections section of the Environment Variables Reference][link-env-var-ref].

[link-env-var-ref]: /reference/environment-variables#database-connection

## Notes

Below you can find useful tips for configuring the connection to specific
databases.

### MongoDB

To use Cube.js with MongoDB you need to install MongoDB Connector for BI. You
can download it [here](https://www.mongodb.com/download-center/bi-connector).
[Learn more about setup for MongoDB here.](https://cube.dev/blog/building-mongodb-dashboard-using-node.js)

### MongoDB Atlas

Use `CUBEJS_DB_SSL=true` to enable SSL as MongoDB Atlas requires it. All other
SSL-related environment variables can be left unset.

### AWS RDS Postgres

Use `CUBEJS_DB_SSL=true` to enable SSL if you have SSL enabled for your RDS
cluster. Download the new certificate [here][link-aws-rds-pem], and provide the
contents of the downloaded file to `CUBEJS_DB_SSL_CA`. All other SSL-related
environment variables can be left unset. See [Enabling SSL][link-enabling-ssl]
for more details. More info on AWS RDS SSL can be found
[here][link-aws-rds-docs].

[link-aws-rds-pem]: https://s3.amazonaws.com/rds-downloads/rds-ca-2019-root.pem
[link-aws-rds-docs]:
  https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.SSL.html

### AWS Athena

For Athena, you'll need to specify the AWS access and secret keys with the
[access necessary to run Athena queries](https://docs.aws.amazon.com/athena/latest/ug/access.html),
and the target AWS region and
[S3 output location](https://docs.aws.amazon.com/athena/latest/ug/querying.html)
where query results are stored.

### Google BigQuery

In order to connect BigQuery to Cube.js, you need to provide service account
credentials. Cube.js requires the service account to have **BigQuery Data
Viewer** and **BigQuery Job User** roles enabled.

You can set the `CUBEJS_DB_BQ_KEY_FILE` environment variable with a path to a
JSON key file.

```dotenv
CUBEJS_DB_BQ_KEY_FILE=/path/to/key-file.json
```

You could also encode the key file with Base64:

```dotenv
CUBEJS_DB_BQ_CREDENTIALS=$(cat /path/to/key-file.json | base64)
```

You can learn more about acquiring Google BigQuery credentials
[here][link-bigquery-getting-started] and [here][link-bigquery-credentials].

[link-bigquery-getting-started]:
  https://cloud.google.com/docs/authentication/getting-started
[link-bigquery-credentials]:
  https://console.cloud.google.com/apis/credentials/serviceaccountkey

### MySQL

To connect to a local MySQL database using a UNIX socket use
`CUBEJS_DB_SOCKET_PATH`, by doing so, `CUBEJS_DB_HOST` will be ignored.

You can connect to an SSL-enabled MySQL database by setting `CUBEJS_DB_SSL` to
`true`. All other SSL-related environment variables can be left unset. See
[Enabling SSL][link-enabling-ssl] for more details.

[link-enabling-ssl]: #enabling-ssl

### Connecting to Multiple Databases

Cube.js supports connection to multiple databases out-of-the-box. Please refer
to [Multitenancy Guide][link-multitenancy] to learn more.

[link-multitenancy]: /multitenancy-setup
