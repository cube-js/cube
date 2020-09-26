---
title: Connecting to the Database
permalink: /connecting-to-the-database
category: Cube.js Backend
menuOrder: 1
---

Cube.js currently provides connectors to the following databases:

| Database                                         | Cube.js DB Type |
| -------------------------------------------------|---------------- |
| PostgreSQL                                       | postgres        |
| MySQL                                            | mysql           |
| AWS Athena                                       | athena          |
| AWS Redshift                                     | redshift        |
| MongoDB (via MongoDB Connector for BI)           | mongobi         |
| Google BigQuery                                  | bigquery        |
| MS SQL                                           | mssql           |
| ClickHouse                                       | clickhouse      |
| Snowflake                                        | snowflake       |
| Presto                                           | prestodb        |
| Hive / SparkSQL (thrift)                         | hive            |
| Oracle                                           | oracle          |
| Apache Druid                                     | druid           |

_If you'd like to connect to a database which is not yet supported, you can create a Cube.js-compilant driver package. [Here's a simple step-by-step guide](https://github.com/cube-js/cube.js/blob/master/CONTRIBUTING.md#implementing-driver)._

### Configuring a Connection for Cube.js CLI Created Apps

When you create a new Cube.js service with the [Cube.js CLI](using-the-cubejs-cli), the `.env` will be
generated to manage all connection credentials. The set of variables could be different based on your database type. For example, for PostgreSQL the `.env` will look like this:


```bash
CUBEJS_DB_HOST=<YOUR_DB_HOST_HERE>
CUBEJS_DB_NAME=<YOUR_DB_NAME_HERE>
CUBEJS_DB_USER=<YOUR_DB_USER_HERE>
CUBEJS_DB_PASS=<YOUR_DB_PASS_HERE>
CUBEJS_DB_TYPE=postgres
CUBEJS_API_SECRET=secret
```

The table below shows which environment variables are used for different databases:

| Database             | Credentials    |
| -------------------- |--------------- |
| PostgreSQL, MySQL, AWS Redshift, ClickHouse, Hive/SparkSQL, Oracle | `CUBEJS_DB_HOST`, `CUBEJS_DB_PORT`, `CUBEJS_DB_NAME`, `CUBEJS_DB_USER`, `CUBEJS_DB_PASS` |
| MS SQL | `CUBEJS_DB_HOST`, `CUBEJS_DB_PORT`, `CUBEJS_DB_NAME`, `CUBEJS_DB_USER`, `CUBEJS_DB_PASS`, `CUBEJS_DB_DOMAIN` |
| AWS Athena | `CUBEJS_AWS_KEY`, `CUBEJS_AWS_SECRET`, `CUBEJS_AWS_REGION`, `CUBEJS_AWS_S3_OUTPUT_LOCATION` |
| Google Bigquery | `CUBEJS_DB_BQ_PROJECT_ID`, `CUBEJS_DB_BQ_KEY_FILE or CUBEJS_DB_BQ_CREDENTIALS` |
| MongoDB | `CUBEJS_DB_HOST`, `CUBEJS_DB_NAME`, `CUBEJS_DB_PORT`, `CUBEJS_DB_USER`, `CUBEJS_DB_PASS`, `CUBEJS_DB_SSL`, `CUBEJS_DB_SSL_CA`, `CUBEJS_DB_SSL_CERT`, `CUBEJS_DB_SSL_CIPHERS`, `CUBEJS_DB_SSL_PASSPHRASE` |
| Snowflake | `CUBEJS_DB_SNOWFLAKE_ACCOUNT`, `CUBEJS_DB_SNOWFLAKE_REGION`, `CUBEJS_DB_SNOWFLAKE_WAREHOUSE`, `CUBEJS_DB_SNOWFLAKE_ROLE`, `CUBEJS_DB_NAME`, `CUBEJS_DB_USER`, `CUBEJS_DB_PASS`|
| Presto | `CUBEJS_DB_HOST`, `CUBEJS_DB_PORT`, `CUBEJS_DB_CATALOG`, `CUBEJS_DB_SCHEMA`, `CUBEJS_DB_USER`, `CUBEJS_DB_PASS` |
| Druid | `CUBEJS_DB_URL`, `CUBEJS_DB_USER`, `CUBEJS_DB_PASS` |

### Configuring a Connection to an External Pre-aggregations Database

To enable [external pre-aggregations](pre-aggregations#external-pre-aggregations) you need to configure an external database to store these pre-aggregations.

Cube.js provides a set of environment variables to configure a connection to an external database:

```bash
CUBEJS_EXT_DB_HOST=<YOUR_DB_HOST_HERE>
CUBEJS_EXT_DB_PORT=<YOUR_DB_PORT_HERE>
CUBEJS_EXT_DB_NAME=<YOUR_DB_NAME_HERE>
CUBEJS_EXT_DB_USER=<YOUR_DB_USER_HERE>
CUBEJS_EXT_DB_PASS=<YOUR_DB_PASS_HERE>
CUBEJS_EXT_DB_TYPE=<SUPPORTED_DB_TYPE_HERE>
```  

## Notes

### MongoDB

To use Cube.js with MongoDB you need to install MongoDB Connector for BI. You
can download it [here](https://www.mongodb.com/download-center/bi-connector). [Learn more about setup for MongoDB
here.](https://cube.dev/blog/building-mongodb-dashboard-using-node.js)

### MongoDB Atlas

Use `CUBEJS_DB_SSL=true` to enable SSL as MongoDB Atlas requires it. `CUBEJS_DB_SSL_CA`, `CUBEJS_DB_SSL_CERT`, `CUBEJS_DB_SSL_CIPHERS`, `CUBEJS_DB_SSL_PASSPHRASE` can be left blank.

### AWS RDS Postgres 

Use `CUBEJS_DB_SSL=true` to enable SSL if you have force ssl enabled for your RDS. 
Download the new certificate [here](https://s3.amazonaws.com/rds-downloads/rds-ca-2019-root.pem) provide the contents of the downloaded file to `CUBEJS_DB_SSL_CA`, `CUBEJS_DB_SSL_CERT`, `CUBEJS_DB_SSL_CIPHERS`, `CUBEJS_DB_SSL_PASSPHRASE` can be left blank. 
More info on AWS RDS SSL can be found [here](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.SSL.html)

### AWS Athena

For Athena, you'll need to specify the AWS access and secret keys with the [access necessary to run Athena queries](https://docs.aws.amazon.com/athena/latest/ug/access.html), and the target AWS region and [S3 output location](https://docs.aws.amazon.com/athena/latest/ug/querying.html) where query results are stored.

### Google BigQuery

In order to connect BigQuery to Cube.js, you need to provide service account credentials.
Cube.js requires the service account to have **BigQuery Data Viewer** and **BigQuery Job User** roles enabled.
You can set `CUBEJS_DB_BQ_KEY_FILE` environment variable with a path to **JSON** key file.

Another way is to encode the key file with **base64**:

```bash
$ cat /path/to/key-file.json | base64
```

Now you can set the `CUBEJS_DB_BQ_CREDENTIALS` environment variable with the base64-encoded key. 

You can learn more about acquiring Google BigQuery credentials [here](https://cloud.google.com/docs/authentication/getting-started) and [here](https://console.cloud.google.com/apis/credentials/serviceaccountkey).

### MySQL

To connect to a local MySQL database using a UNIX socket use `CUBEJS_DB_SOCKET_PATH`, by doing so, `CUBEJS_DB_HOST` will be ignored.

You can connect to a SSL enabled MySQL database by setting `CUBEJS_DB_SSL` to `true`.  `CUBEJS_DB_SSL_CA`, `CUBEJS_DB_SSL_CERT`, `CUBEJS_DB_SSL_CIPHERS` and `CUBEJS_DB_SSL_PASSPHRASE` can be used according to your requirements.

### Connecting to Multiple Databases

Cube.js supports connection to multiple databases out-of-the-box. Please refer to [Multitenancy Guide](multitenancy-setup) to learn more.

### SSL

Cube.js supports connection via SSL for **Postgres**, **Mongo** and **MySQL**. To enable it set
`CUBEJS_DB_SSL` environment variable to `true`.
