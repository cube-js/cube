---
title: Environment Variables
permalink: /reference/environment-variables
category: Cube.js Backend
menuOrder: 3
---

Cube.js defines a number of environment variables that can be used to change
behavior. Some of these variables can also be set via [configuration options][link-config].

[link-config]: /config

## General

|Environment variable                |Description                                                                                                                                                     |Possible Values                                          |
|------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------|
|`CUBEJS_CACHE_AND_QUEUE_DRIVER`     |The cache and queue driver to use for the Cube.js deployment. Defaults to `redis`                                                                               |`redis | memory`                                         |
|`CUBEJS_API_SECRET`                 |The secret key used to sign and verify JWTs. Generated on project scaffold                                                                                      |A valid string                                           |
|`CUBEJS_APP`                        |An application ID used to uniquely identify the Cube.js deployment. Can be different for multitenant setups. Defaults to `cubejs`                               |A valid string                                           |
|`CUBEJS_SCHEDULED_REFRESH_TIMER`    |If `true`, enabled scheduled refreshes. Can also be set to a number representing how many seconds to wait before triggering another refresh. Defaults to `false`|`true | false` or a valid number of seconds              |
|`CUBEJS_SCHEDULED_REFRESH_TIMEZONES`|A comma-separated [list of timezones to schedule refreshes for](/config#options-reference-scheduled-refresh-timer).                                                                                                   |[A valid timezone from the tz database][link-tz-database]|
|`CUBEJS_SCHEMA_PATH`                |The path where Cube.js loads schemas from. Defaults to `schema`                                                                                                 |A valid folder containing Cube.js schemas                |
|`CUBEJS_TELEMETRY`                  |If `true`, then send telemetry to CubeJS. Defaults to `true`                                                                                                    |`true | false`                                           |
|`CUBEJS_WEB_SOCKETS`                |If `true`, then use WebSocket for data fetching. Defaults to `true`                                                                                             |`true | false`                                           |
|`NODE_ENV`                          |The environment that CubeJS is running in. Defaults to `production`                                                                                             |`production | development | test`                        |
|`PORT`                              |The port for a Cube.js deployment to listen to API connections on. Defaults to `4000`                                                                           |A valid port number                                      |
|`CUBEJS_LOG_LEVEL`                  |The logging level for Cube.js. Defaults to `warn`                                                                                                               |`error | info | trace | warn`                            |
|`DEBUG_LOG`                         |If `true`, enables debug logging. Defaults to `false`.                                                                                                          |`true | false`                                           |
|`REDIS_URL`                         |The host URL for a Redis server                                                                                                                                 |A valid Redis host URL                                   |
|`REDIS_PASSWORD`                    |The password used to connect to the Redis server                                                                                                                |A valid Redis password                                   |
|`REDIS_TLS`                         |If `true`, then the connection to the Redis server is protected by TLS authentication. Defaults to `false`                                                      |`true | false`                                           |
|`CUBEJS_REDIS_POOL_MAX`             |The maximum number of connections to keep active in the Redis connection pool. Must be higher than CUBEJS_REDIS_POOL_MIN. Defaults to `1000`                    |A valid number of connections.                           |
|`CUBEJS_REDIS_POOL_MIN`             |The minimum number of connections to keep active in the Redis connection pool. Must be lower than CUBEJS_REDIS_POOL_MAX. Defaults to `2`                        |A valid number of connections                            |

[link-tz-database]: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones

## Database Connection

The following environment variables are used to provide credentials for Cube.js to connect to the databases. You can [learn more about connecting to the databases in this guide.](connecting-to-the-database)

|Environment variable               |Used With           |Description                                                                     |Possible Values                                                                                                                |
|-----------------------------------|--------------------|--------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
|`CUBEJS_AWS_KEY`                   |AWS Athena, JDBC    |The AWS Access Key ID to use for database connections                           |A valid AWS Access Key ID                                                                                                      |
|`CUBEJS_AWS_REGION`                |AWS Athena, JDBC    |The AWS region of the Cube.js deployment                                        |[A valid AWS region][link-aws-regions]                                                                                         |
|`CUBEJS_AWS_S3_OUTPUT_LOCATION`    |AWS Athena, JDBC    |The S3 path to store query results made by the Cube.js deployment               |A valid S3 path                                                                                                                |
|`CUBEJS_AWS_SECRET`                |AWS Athena, JDBC    |The AWS Secret Access Key to use for database connections                       |A valid AWS Secret Access Key                                                                                                  |
|`CUBEJS_DB_BQ_CREDENTIALS`         |BigQuery            |A Base64 encoded JSON key file for connecting to Google BigQuery                |A valid Google BigQuery JSON key file encoded as a Base64 string                                                               |
|`CUBEJS_DB_BQ_KEY_FILE`            |BigQuery            |The path to a JSON key file for connecting to Google BigQuery                   |A valid Google BigQuery JSON key file                                                                                          |
|`CUBEJS_DB_BQ_PROJECT_ID`          |BigQuery            |The Google BigQuery project ID to connect to                                    |A valid Google BigQuery Project ID                                                                                             |
|`CUBEJS_DB_URL`                    |Druid, Elasticsearch|The URL for a database                                                          |A valid database URL for Druid/Elasticsearch                                                                                   |
|`CUBEJS_DB_ELASTIC_OPENDISTRO`     |Elasticsearch       |If `true`, then use the Open Distro for Elasticsearch                           |`true | false`                                                                                                                 |
|`CUBEJS_EXT_DB_HOST`               |External            |The host URL for an external pre-aggregations database                          |A valid database host URL                                                                                                      |
|`CUBEJS_EXT_DB_NAME`               |External            |The name of the external pre-aggregations database to connect to                |A valid database name                                                                                                          |
|`CUBEJS_EXT_DB_PASS`               |External            |The password used to connect to the external pre-aggregations database          |A valid database password                                                                                                      |
|`CUBEJS_EXT_DB_PORT`               |External            |The port for the external pre-aggregations database                             |A valid port number                                                                                                            |
|`CUBEJS_EXT_DB_TYPE`               |External            |A database type supported by Cube.js                                            |[A valid database supported by Cube.js][link-cubejs-databases]                                                                 |
|`CUBEJS_EXT_DB_USER`               |External            |The username used to connect to the external pre-aggregations database          |A valid database username                                                                                                      |
|`CUBEJS_DB_SSL`                    |General             |If `true`, enables SSL encryption for database connections from Cube.js         |`true | false`                                                                                                                 |
|`CUBEJS_DB_SSL_REJECT_UNAUTHORIZED`|General             |If `true`, verifies the CA chain with the system's built-in CA chain            |`true | false`                                                                                                                 |
|`CUBEJS_DB_TYPE`                   |General             |A database type supported by Cube.js                                            |[A valid database supported by Cube.js][link-cubejs-databases]                                                                 |
|`CUBEJS_DB_USER`                   |General             |The username used to connect to the database                                    |A valid database username                                                                                                      |
|`CUBEJS_DB_HOST`                   |General             |The host URL for a database                                                     |A valid database host URL                                                                                                      |
|`CUBEJS_DB_MAX_POOL`               |General             |The maximum number of connections to keep active in the database connection pool|A valid number of connections                                                                                                  |
|`CUBEJS_DB_NAME`                   |General             |The name of the database to connect to                                          |A valid database name                                                                                                          |
|`CUBEJS_DB_PASS`                   |General             |The password used to connect to the database                                    |A valid database password                                                                                                      |
|`CUBEJS_DB_PORT`                   |General             |The port for the database connection                                            |A valid port number                                                                                                            |
|`CUBEJS_DB_SCHEMA`                 |General             |The schema within the database to connect to                                    |A valid schema name within a Presto database                                                                                   |
|`CUBEJS_DB_HIVE_CDH_VER`           |Hive                |The version of the CDH instance for Apache Hive                                 |[A valid CDH version][link-hive-cdh-versions]                                                                                  |
|`CUBEJS_DB_HIVE_THRIFT_VER`        |Hive                |The version of Thrift Server for Apache Hive                                    |[A valid Thrift Server version][link-hive-thrift-versions]                                                                     |
|`CUBEJS_DB_HIVE_TYPE`              |Hive                |The type of Apache Hive server                                                  |`CDH | HIVE`                                                                                                                   |
|`CUBEJS_DB_HIVE_VER`               |Hive                |The version of Apache Hive                                                      |[A valid Apache Hive version][link-hive-versions]                                                                              |
|`CUBEJS_JDBC_DRIVER`               |JDBC                |                                                                                |`athena`                                                                                                                       |
|`CUBEJS_JDBC_URL`                  |JDBC                |The URL for a JDBC connection                                                   |A valid JDBC URL                                                                                                               |
|`CUBEJS_DB_DOMAIN`                 |MSSQL               |A domain name within the database to connect to                                 |A valid domain name within a MSSQL database                                                                                    |
|`CUBEJS_DB_SOCKET_PATH`            |MySQL               |The path to a Unix socket for a MySQL database                                  |A valid path to a Unix socket for a MySQL database                                                                             |
|`CUBEJS_DB_CATALOG`                |Presto              |The catalog within the database to connect to                                   |A valid catalog name within a Presto database                                                                                  |
|`CUBEJS_DB_SNOWFLAKE_ACCOUNT`      |Snowflake           |The Snowflake account ID to use when connecting to the database                 |A valid Snowflake account ID                                                                                                   |
|`CUBEJS_DB_SNOWFLAKE_REGION`       |Snowflake           |The Snowflake region to use when connecting to the database                     |[A valid Snowflake region][link-snowflake-regions]                                                                             |
|`CUBEJS_DB_SNOWFLAKE_ROLE`         |Snowflake           |The Snowflake role to use when connecting to the database                       |A valid Snowflake role for the account                                                                                         |
|`CUBEJS_DB_SNOWFLAKE_WAREHOUSE`    |Snowflake           |The Snowflake warehouse to use when connecting to the database                  |A valid Snowflake warehouse for the account                                                                                    |

[link-aws-regions]: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-available-regions
[link-cubejs-databases]: /connecting-to-the-database
[link-hive-cdh-versions]: https://docs.cloudera.com/documentation/enterprise/6/release-notes/topics/rg_cdh_6_download.html
[link-hive-thrift-versions]: https://github.com/apache/thrift/releases
[link-hive-versions]: https://hive.apache.org/downloads.html
[link-snowflake-regions]: https://docs.snowflake.com/en/user-guide/intro-regions.html
