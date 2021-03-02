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

| Database                                               | Credentials                                                                                                                                                                                                                     |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| PostgreSQL, MySQL, AWS Redshift, Hive/SparkSQL, Oracle | `CUBEJS_DB_HOST`, `CUBEJS_DB_PORT`, `CUBEJS_DB_NAME`, `CUBEJS_DB_USER`, `CUBEJS_DB_PASS`                                                                                                                                        |
| MS SQL                                                 | `CUBEJS_DB_HOST`, `CUBEJS_DB_PORT`, `CUBEJS_DB_NAME`, `CUBEJS_DB_USER`, `CUBEJS_DB_PASS`, `CUBEJS_DB_DOMAIN`                                                                                                                    |
| ClickHouse                                             | `CUBEJS_DB_HOST`, `CUBEJS_DB_PORT`, `CUBEJS_DB_NAME`, `CUBEJS_DB_USER`, `CUBEJS_DB_PASS`, `CUBEJS_DB_SSL`, `CUBEJS_DB_CLICKHOUSE_READONLY`                                                                                      |
| AWS Athena                                             | `CUBEJS_AWS_KEY`, `CUBEJS_AWS_SECRET`, `CUBEJS_AWS_REGION`, `CUBEJS_AWS_S3_OUTPUT_LOCATION`                                                                                                                                     |
| Google BigQuery                                        | `CUBEJS_DB_BQ_PROJECT_ID`, `CUBEJS_DB_BQ_KEY_FILE or CUBEJS_DB_BQ_CREDENTIALS`                                                                                                                                                  |
| MongoDB                                                | `CUBEJS_DB_HOST`, `CUBEJS_DB_NAME`, `CUBEJS_DB_PORT`, `CUBEJS_DB_USER`, `CUBEJS_DB_PASS`, `CUBEJS_DB_SSL`, `CUBEJS_DB_SSL_CA`, `CUBEJS_DB_SSL_CERT`, `CUBEJS_DB_SSL_CIPHERS`, `CUBEJS_DB_SSL_PASSPHRASE`                        |
| Snowflake                                              | `CUBEJS_DB_SNOWFLAKE_ACCOUNT`, `CUBEJS_DB_SNOWFLAKE_REGION`, `CUBEJS_DB_SNOWFLAKE_WAREHOUSE`, `CUBEJS_DB_SNOWFLAKE_ROLE`, `CUBEJS_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE`, `CUBEJS_DB_NAME`, `CUBEJS_DB_USER`, `CUBEJS_DB_PASS` |
| Presto                                                 | `CUBEJS_DB_HOST`, `CUBEJS_DB_PORT`, `CUBEJS_DB_CATALOG`, `CUBEJS_DB_SCHEMA`, `CUBEJS_DB_USER`, `CUBEJS_DB_PASS`                                                                                                                 |
| Druid                                                  | `CUBEJS_DB_URL`, `CUBEJS_DB_USER`, `CUBEJS_DB_PASS`, `CUBEJS_DB_SSL`                                                                                                                                                            |
| SQLite                                                 | `CUBEJS_DB_NAME`                                                                                                                                                                                                                |

## Multiple Databases

Cube.js supports connection to multiple databases out-of-the-box. Please refer
to [Multitenancy Guide][link-multitenancy] to learn more.

[link-multitenancy]: /multitenancy-setup

## Enabling SSL

Cube.js supports SSL-encrypted connections for **ClickHouse**, **Postgres**, **MongoDB**, **MS
SQL**, and **MySQL**. To enable it set the `CUBEJS_DB_SSL` environment variable
to `true`. Cube.js can also be configured to use custom connection settings. For
example, to use a custom CA and certificates, you could do the following:

```dotenv
CUBEJS_DB_SSL_CA=/ssl/ca.pem
CUBEJS_DB_SSL_CERT=/ssl/cert.pem
CUBEJS_DB_SSL_KEY=/ssl/key.pem
```

You can also set the above environment variables to the contents of the PEM
files; for example:

```dotenv
CUBEJS_DB_SSL_CA="-----BEGIN CERTIFICATE-----
MIIDDjCCAfYCCQCN/HhSZ3ofTDANBgkqhkiG9w0BAQsFADBJMQswCQYDVQQGEwJV
SzEMMAoGA1UECgwDSUJNMQ0wCwYDVQQLDARBSU9TMR0wGwYDVQQDDBRhaW9zLW9y
Y2gtZGV2LWVudi1DQTAeFw0yMTAyMTUyMzIyMTZaFw0yMzEyMDYyMzIyMTZaMEkx
CzAJBgNVBAYTAlVLMQwwCgYDVQQKDANJQk0xDTALBgNVBAsMBEFJT1MxHTAbBgNV
BAMMFGFpb3Mtb3JjaC1kZXYtZW52LUNBMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8A
MIIBCgKCAQEAyhYY9+4TduTsNRh/6MaRtE59j8HkAkoQYvNYZN7D1j1oV6yhzitn
oN4bD+HiQWe4J3mwAaJOAAJRCkIVyUXxwZUCPxGN/KVha/pcB8hN6LHfI6vInixp
U9kHNYWWBn428nMeMqts7yqly/HwG1/qO+j4178c8lZNS7Uwh76y+lAEaIkeBipq
i4WuCOiChFc/sIV7g4DcLKKbqzDWtRDjbsg7JRfsALO5gM360GrNYkhV4C5lm8Eh
ozNuaPhS65zO93PMj/3UTyuctXKa7WpaHJHoKZRXAuOwSamvqvFgIQ0SSnW+qcud
fL3GAPJn7d065gh7JvgcT86v7WWBiUNs0QIDAQABMA0GCSqGSIb3DQEBCwUAA4IB
AQCzw00d8e0e5AYZtzIk9hjczta7JHy2/cwTMv0opzBk6C26G6YZww+9brHW2w5U
mY/HKBnGnMadjMWOZmm9Vu0B0kalYY0lJdE8alO1aiv5B9Ms/XIt7FzzGtfv9gYJ
cw5/nzGBBMJNICC1kVLnzzlllLferhCIrczDyPcu16o1Flc7q1p8AbwQpC+A2I/L
8nWlFeHZ+watLtQ1lF3qDzzCumPHrJqAGmlp0265owCM8Q5zv8AL5DStIZvtexrI
JqbwLdbA8smyOFRwCckOWcWjnrEDjO2e3NLWINbB7Z4ZRviZSEH5UZlDLVu+ahGV
KmZIuh7+XpXzJ1MN0SBZXgXH
-----END CERTIFICATE-----"
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

### Google Cloud SQL Postgres

You can connect to an SSL-enabled MySQL database by setting `CUBEJS_DB_SSL` to
`true`. You may also need to set `CUBEJS_DB_SSL_SERVERNAME`, depending on how
you are [connecting to Cloud SQL][link-cloud-sql-connect].

[link-cloud-sql-connect]:
  https://cloud.google.com/sql/docs/postgres/connect-functions#connecting_to

### Heroku Postgres

Unless you're using a Private or Shield Heroku Postgres database, Heroku Postgres does not currently support verifiable certificates. [Here is the description of the issue from Heroku](https://help.heroku.com/3DELT3RK/why-can-t-my-third-party-utility-connect-to-heroku-postgres-with-ssl).

As a workaround you can set `rejectUnauthorized` option to `false` in the
Cube.js Postgres driver.

```js
const PostgresDriver = require('@cubejs-backend/postgres-driver');
module.exports = {
  driverFactory: () => new PostgresDriver({
    ssl: {
      rejectUnauthorized: false
    }
  })
};
```

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

### Druid

You can connect to an HTTPS-enabled Druid database by setting `CUBEJS_DB_SSL` to
`true`. All other SSL-related environment variables can be left unset. See
[Enabling SSL][link-enabling-ssl] for more details.

### ClickHouse

You can connect to an HTTPS-enabled ClickHouse database by setting `CUBEJS_DB_SSL` to
`true`. All other SSL-related environment variables can be left unset. See
[Enabling SSL][link-enabling-ssl] for more details.

You can connect to a ClickHouse database when your user's permissions are
[restricted][link-clickhouse-readonly] to read-only, by setting `CUBEJS_DB_CLICKHOUSE_READONLY`
to `true`.

[link-enabling-ssl]: #enabling-ssl
[link-clickhouse-readonly]: https://clickhouse.tech/docs/en/operations/settings/permissions-for-queries/#settings_readonly

