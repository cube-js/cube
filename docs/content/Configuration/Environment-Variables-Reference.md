---
title: Environment Variables
permalink: /reference/environment-variables
category: Configuration
subCategory: Reference
menuOrder: 4
---

Cube.js defines a number of environment variables that can be used to change
behavior. Some of these variables can also be set via [configuration
options][link-config].

[link-config]: /config

## General

| Environment variable                   | Description                                                                                                                                                                      | Possible Values                                                                                                                   |
| -------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `CUBEJS_CACHE_AND_QUEUE_DRIVER`        | The cache and queue driver to use for the Cube.js deployment. Defaults to `redis`                                                                                                | `redis`, `memory`                                                                                                                 |
| `CUBEJS_DEV_MODE`                      | If `true`, enables development mode. Defaults to `false`                                                                                                                         | `true`, `false`                                                                                                                   |
| `CUBEJS_API_SECRET`                    | The secret key used to sign and verify JWTs. Generated on project scaffold                                                                                                       | A valid string                                                                                                                    |
| `CUBEJS_APP`                           | An application ID used to uniquely identify the Cube.js deployment. Can be different for multitenant setups. Defaults to `cubejs`                                                | A valid string                                                                                                                    |
| `CUBEJS_REFRESH_WORKER`                | If `true`, this instance of Cube.js will **only** refresh pre-aggregations. Defaults to `false`                                                                                  | `true`, `false`                                                                                                                   |
| `CUBEJS_ROLLUP_ONLY`                   | If `true`, this instance of Cube.js will **only** query rollup pre-aggregations. Defaults to `false`                                                                             | `true`, `false`                                                                                                                   |
| `CUBEJS_SCHEDULED_REFRESH_TIMEZONES`   | A comma-separated [list of timezones to schedule refreshes for][ref-config-sched-refresh-timer].                                                                                 | [A valid timezone from the tz database][link-tz-database]                                                                         |
| `CUBEJS_SCHEDULED_REFRESH_CONCURRENCY` | How many pre-aggregations refresh worker will build in parallel. Please note changing this param doesn't change queue concurrency and it should be adjusted accordingly          | A valid number of concurrent refresh processes                                                                                    |
| `CUBEJS_PRE_AGGREGATIONS_SCHEMA`       | The [schema name][ref-config-preagg-schema-name] to use for storing pre-aggregations. Defaults to `dev_pre_aggregations`/`prod_pre_aggregations` for development/production mode | A valid string                                                                                                                    |
| `CUBEJS_SCHEMA_PATH`                   | The path where Cube.js loads schemas from. Defaults to `schema`                                                                                                                  | A valid folder containing Cube.js schemas                                                                                         |
| `CUBEJS_TELEMETRY`                     | If `true`, then send telemetry to CubeJS. Defaults to `true`                                                                                                                     | `true`, `false`                                                                                                                   |
| `CUBEJS_WEB_SOCKETS`                   | If `true`, then use WebSocket for data fetching. Defaults to `true`                                                                                                              | `true`, `false`                                                                                                                   |
| `PORT`                                 | The port for a Cube.js deployment to listen to API connections on. Defaults to `4000`                                                                                            | A valid port number                                                                                                               |
| `CUBEJS_LOG_LEVEL`                     | The logging level for Cube.js. Defaults to `warn`                                                                                                                                | `error`, `info`, `trace`, `warn`                                                                                                  |
| `DEBUG_LOG`                            | If `true`, enables debug logging. Defaults to `false`                                                                                                                            | `true`, `false`                                                                                                                   |
| `CUBEJS_DB_QUERY_TIMEOUT`              | The timeout value for any queries made to the database by Cube. The value can be a number in seconds or a duration string. Defaults to `10m`                                     | `5000`, `1s`, `1m`, `1h`                                                                                                          |
| `CUBEJS_REDIS_URL`                     | The host URL for a Redis server                                                                                                                                                  | A valid Redis host URL                                                                                                            |
| `CUBEJS_REDIS_PASSWORD`                | The password used to connect to the Redis server                                                                                                                                 | A valid Redis password                                                                                                            |
| `CUBEJS_REDIS_TLS`                     | If `true`, then the connection to the Redis server is protected by TLS authentication. Defaults to `false`                                                                       | `true`, `false`                                                                                                                   |
| `CUBEJS_REDIS_POOL_MAX`                | The maximum number of connections to keep active in the Redis connection pool for a single `appId` (tenant). Must be higher than `CUBEJS_REDIS_POOL_MIN`. Defaults to `1000`     | A valid number of connections.                                                                                                    |
| `CUBEJS_REDIS_POOL_MIN`                | The minimum number of connections to keep active in the Redis connection pool for a single `appId` (tenant). Must be lower than `CUBEJS_REDIS_POOL_MAX`. Defaults to `2`         | A valid number of connections                                                                                                     |
| `CUBEJS_REDIS_USE_IOREDIS`             | Use [`ioredis`][gh-ioredis] instead of[ `redis`][gh-node-redis]. Defaults to `false`                                                                                             | `true`, `false`                                                                                                                   |
| `CUBEJS_JWK_URL`                       | A valid URL to a JSON Web Key Sets (JWKS)                                                                                                                                        | `https://<AUTH0-SUBDOMAIN>.auth0.com/.well-known/jwks.json`                                                                       |
| `CUBEJS_JWT_KEY`                       | The secret key used to sign and verify JWTs. Similar to `CUBEJS_API_SECRET`                                                                                                      | A valid string                                                                                                                    |
| `CUBEJS_JWT_AUDIENCE`                  | An audience value which will be used to enforce the [`aud` claim from inbound JWTs][link-jwt-ref-aud]                                                                            | `https://myapp.com`                                                                                                               |
| `CUBEJS_JWT_ISSUER`                    | An issuer value which will be used to enforce the [`iss` claim from inbound JWTs][link-jwt-ref-iss]                                                                              | `https://<AUTH0-SUBDOMAIN>.auth0.com/`                                                                                            |
| `CUBEJS_JWT_SUBJECT`                   | A subject value which will be used to enforce the [`sub` claim from inbound JWTs][link-jwt-ref-sub]                                                                              | `person@example.com`                                                                                                              |
| `CUBEJS_JWT_ALGS`                      | [Any supported algorithm for decoding JWTs][gh-jsonwebtoken-algs]                                                                                                                | `HS256`, `RS256`                                                                                                                  |
| `CUBEJS_JWT_CLAIMS_NAMESPACE`          | A namespace within the decoded JWT under which any custom claims can be found                                                                                                    | `https://myapp.com`                                                                                                               |
| `CUBEJS_CUBESTORE_HOST`                | The hostname of the Cube Store deployment                                                                                                                                        | A valid hostname                                                                                                                  |
| `CUBEJS_CUBESTORE_PORT`                | The port of the Cube Store deployment                                                                                                                                            | A valid port number                                                                                                               |
| `CUBEJS_TOPIC_NAME`                    | The name of the Amazon SNS or Google Cloud Pub/Sub topic (defaults to `<process.env.CUBEJS_APP>-process` if undefined, and finally `cubejs-process`)                             | A valid topic name                                                                                                                |
| `CUBEJS_GH_API_TOKEN`                  | A Github Personal Token to avoid Github API rate limit at downloading cubestore                                                                                                  | It can be a personal access token, an OAuth token, an installation access token or a JSON Web Token for GitHub App authentication |

[ref-config-sched-refresh-timer]: /config#scheduled-refresh-timer
[ref-config-preagg-schema-name]: /config#pre-aggregations-schema
[gh-ioredis]: https://github.com/luin/ioredis
[gh-node-redis]: https://github.com/NodeRedis/node-redis
[link-tz-database]: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
[link-jwt-ref-iss]: https://tools.ietf.org/html/rfc7519#section-4.1.1
[link-jwt-ref-sub]: https://tools.ietf.org/html/rfc7519#section-4.1.2
[link-jwt-ref-aud]: https://tools.ietf.org/html/rfc7519#section-4.1.3
[gh-jsonwebtoken-algs]:
  https://github.com/auth0/node-jsonwebtoken#algorithms-supported
[link-jwk-ref]: https://tools.ietf.org/html/rfc7517#section-4
[link-preaggregations-storage]:
  /caching/using-pre-aggregations#pre-aggregations-storage

## Database Connection

To see a complete list of environment variables for your specific database,
please use the [database connection guide][link-connecting-to-db].

[link-connecting-to-db]: /connecting-to-the-database

## Export Bucket

| Environment variable                   | Description                                                                                  | Possible Values                                                  |
| -------------------------------------- | -------------------------------------------------------------------------------------------- | ---------------------------------------------------------------- |
| `CUBEJS_DB_EXPORT_BUCKET`              | The name of a bucket in cloud storage                                                        | `exports-20210505`                                               |
| `CUBEJS_DB_EXPORT_BUCKET_TYPE`         | The cloud provider where the bucket is hosted                                                | `gcp`, `s3`                                                      |
| `CUBEJS_DB_EXPORT_BUCKET_AWS_KEY`      | The AWS Access Key ID to use for the export bucket                                           | A valid AWS Access Key ID                                        |
| `CUBEJS_DB_EXPORT_BUCKET_AWS_SECRET`   | The AWS Secret Access Key to use for the export bucket                                       | A valid AWS Secret Access Key                                    |
| `CUBEJS_DB_EXPORT_BUCKET_AWS_REGION`   | The AWS region of the export bucket                                                          | [A valid AWS region][link-aws-regions]                           |
| `CUBEJS_DB_EXPORT_BUCKET_REDSHIFT_ARN` | ARN of iam_role with permission to write to the provided `bucket`                            | A valid ARN to an IAM Role associated to the target Redshift DB  |
| `CUBEJS_DB_EXPORT_GCS_CREDENTIALS`     | A Base64 encoded JSON key file for connecting to Google Cloud                                | A valid Google Cloud JSON key file encoded as a Base64 string    |
| `CUBEJS_DB_EXPORT_INTEGRATION`         | The name of the integration used in the database. Only required when using Snowflake and GCS | A valid string matching the name of the integration in Snowflake |

## SQL API

| Environment variable    | Description                                                            | Possible Values     |
|-------------------------|------------------------------------------------------------------------|---------------------|
| `CUBEJS_SQL_USER`       | Required username to access SQL API                                    | A valid string      |
| `CUBEJS_SQL_PASSWORD`   | Required password to access SQL API                                    | A valid string      |
| `CUBEJS_SQL_PORT`       | The port to listen to MySQL compatibility connections on.              | A valid port number |
| `CUBEJS_PG_SQL_PORT`    | The port to listen to PostgreSQL compatibility connections on.         | A valid port number |
| `CUBEJS_SQL_SUPER_USER` | A name of specific user who will be allowed to change security context | A valid string      |

## Cube Store

| Environment variable            | Description                                                                                                                                                                   | Possible Values                                             |
| ------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------- |
| `CUBESTORE_BIND_ADDR`           | The address/port pair for Cube Store's MySQL-compatible interface. Defaults to `0.0.0.0:3306`                                                                                 | A valid address/port pair                                   |
| `CUBESTORE_DATA_DIR`            | A path on the local filesystem to store a local replica of the data. Must be unique on each node and different from `CUBESTORE_REMOTE_DIR`. Defaults to `.cubestore/data`     | A valid path on the local filesystem with read/write access |
| `CUBESTORE_HTTP_BIND_ADDR`      | The address/port pair for Cube Store's HTTP interface. Defaults to `0.0.0.0:3030`                                                                                             | A valid address/port pair                                   |
| `CUBESTORE_HTTP_PORT`           | The port for Cube Store to listen to HTTP connections on. Ignored when `CUBESTORE_HTTP_BIND_ADDR` is set. Defaults to `3030`                                                  | A valid port number                                         |
| `CUBESTORE_JOB_RUNNERS`         | The number of parallel tasks that process non-interactive jobs like data insertion, compaction etc. Defaults to `4`                                                           | A valid number                                              |
| `CUBESTORE_LOG_LEVEL`           | The logging level for Cube Store. Defaults to `error`                                                                                                                         | `error`, `warn`, `info`, `debug`, `trace`                   |
| `CUBESTORE_META_ADDR`           | The address/port pair for the **router** node in the cluster                                                                                                                  | A valid address/port pair                                   |
| `CUBESTORE_META_PORT`           | The port for the **router** node to listen for connections on. Ignored when `CUBESTORE_META_ADDR` is set.                                                                     | A valid port number                                         |
| `CUBESTORE_NO_UPLOAD`           | If `true`, prevents uploading serialized pre-aggregations to cloud storage                                                                                                    | `true`, `false`                                             |
| `CUBESTORE_PORT`                | The port for Cube Store to listen to connections on. Ignored when `CUBESTORE_BIND_ADDR` is set. Defaults to `3306`                                                            | A valid port number                                         |
| `CUBESTORE_QUERY_TIMEOUT`       | The timeout for SQL queries in seconds. Defaults to `120`                                                                                                                     | A number in seconds                                         |
| `CUBESTORE_REMOTE_DIR`          | A path on the local filesystem to store metadata and datasets from all nodes as if it were remote storage. Not required if using GCS/S3. Not recommended for production usage | A valid path on the local filesystem with read/write access |
| `CUBESTORE_SELECT_WORKERS`      | The number of Cube Store sub-processes that handle `SELECT` queries. Defaults to `4`                                                                                          | A valid number                                              |
| `CUBESTORE_SERVER_NAME`         | The full name and port number of the Cube Store server. Must be unique for each instance in cluster mode. Defaults to `localhost`                                             | A valid address/port pair                                   |
| `CUBESTORE_WAL_SPLIT_THRESHOLD` | The maximum number of rows to keep in a single chunk of data right after insertion. Defaults to `262144`                                                                      | A valid number                                              |
| `CUBESTORE_WORKER_PORT`         | The port for Cube Store workers to listen to connections on. When set, the node will start as a **worker** in the cluster                                                     | A valid port number                                         |
| `CUBESTORE_WORKERS`             | A comma-separated list of address/port pairs; for example `worker-1:3123,localhost:3124,123.124.125.128:3123`                                                                 | A comma-separated list of address/port pairs                |

### <--{"id" : "Cube Store"}--> Cloud Storage

| Environment variable                       | Description                                                                                                                             | Possible Values                                                                         |
| ------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------- | --- |
| `CUBESTORE_AWS_ACCESS_KEY_ID`              | The Access Key ID for AWS. Required when using AWS S3                                                                                   | [A valid AWS Access Key ID][link-aws-creds]                                             |
| `CUBESTORE_AWS_SECRET_ACCESS_KEY`          | The Secret Access Key for AWS. Required when using AWS S3                                                                               | [A valid AWS Secret Access Key][link-aws-creds]                                         |
| `CUBESTORE_AWS_CREDS_REFRESH_EVERY_MINS`   | The number of minutes after which Cube Store should refresh AWS credentials. Required when using an AWS instance role. Default is `180` | A valid number in minutes                                                               |
| `CUBESTORE_S3_BUCKET`                      | The name of a bucket in AWS S3. Required when using AWS S3                                                                              | A valid bucket name in the AWS account                                                  |
| `CUBESTORE_S3_REGION`                      | The region of a bucket in AWS S3. Required when using AWS S3                                                                            | [A valid AWS region][link-aws-regions]                                                  |
| `CUBESTORE_S3_SUB_PATH`                    | The path in a AWS S3 bucket to store pre-aggregations. Optional                                                                         | -                                                                                       |
| `CUBESTORE_GCP_CREDENTIALS`                | A Base64 encoded JSON key file for connecting to Google Cloud. Required when using Google Cloud Storage                                 | [A valid Google BigQuery JSON key file encoded as a Base64 string][link-gcp-creds-json] |
| `CUBESTORE_GCP_KEY_FILE`                   | The path to a JSON key file for connecting to Google Cloud. Required when using Google Cloud Storage                                    | [A valid Google Cloud JSON key file][link-gcp-creds-json]                               |
| `CUBESTORE_GCS_BUCKET`                     | The name of a bucket in GCS. Required when using GCS                                                                                    | A valid bucket name in the Google Cloud account                                         |
| `CUBESTORE_GCS_SUB_PATH`                   | The path in a GCS bucket to store pre-aggregations. Optional                                                                            | -                                                                                       |
| `CUBESTORE_MINIO_ACCESS_KEY_ID`            | The Access Key ID for minIO. Required when using minIO                                                                                  | A valid minIO Access Key ID                                                             |
| `CUBESTORE_MINIO_SECRET_ACCESS_KEY`        | The Secret Access Key for minIO. Required when using minIO                                                                              | A valid minIO Secret Access Key                                                         |     |
| `CUBESTORE_MINIO_BUCKET`                   | The name of the bucket that you want to use minIO. Required when using minIO                                                            | A valid bucket name in the AWS account                                                  |
| `CUBESTORE_MINIO_REGION`                   | The region of a bucket in S3 that you want to use minIO. Optional when using minIO                                                      | A valid S3 region name, an empty string if not present                                  |
| `CUBESTORE_MINIO_SERVER_ENDPOINT`          | The minIO server endpoint. Required when using minIO                                                                                    | A valid minIO endpoint e.g. `http://localhost:9000`                                     |
| `CUBESTORE_MINIO_CREDS_REFRESH_EVERY_MINS` | The number of minutes after which Cube Store should refresh minIO credentials. Default is `180`                                         | A valid number in minutes                                                               |

[link-aws-creds]:
  https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html#access-keys-and-secret-access-keys
[link-aws-regions]:
  https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-available-regions
[link-aws-athena-workgroup]:
  https://docs.aws.amazon.com/athena/latest/ug/workgroups-benefits.html
[link-elastic-api-keys]:
  https://www.elastic.co/guide/en/kibana/master/api-keys.html#create-api-key
[link-gcp-creds-json]:
  https://cloud.google.com/iam/docs/creating-managing-service-account-keys
[link-cubejs-databases]: /connecting-to-the-database
[link-nodejs-tls-options]:
  https://nodejs.org/docs/latest/api/tls.html#tls_tls_createsecurecontext_options
[link-nodejs-tls-connect-opts]:
  https://nodejs.org/docs/latest/api/tls.html#tls_tls_connect_options_callback
[link-nodejs-tls-ciphers]:
  https://nodejs.org/docs/latest/api/tls.html#tls_modifying_the_default_tls_cipher_suite
[link-hive-cdh-versions]:
  https://docs.cloudera.com/documentation/enterprise/6/release-notes/topics/rg_cdh_6_download.html
[link-hive-thrift-versions]: https://github.com/apache/thrift/releases
[link-hive-versions]: https://hive.apache.org/downloads.html
[link-snowflake-account]:
  https://docs.getdbt.com/reference/warehouse-profiles/snowflake-profile#account
[link-snowflake-regions]:
  https://docs.snowflake.com/en/user-guide/intro-regions.html
[link-snowflake-connection-options]:
  https://docs.snowflake.com/en/user-guide/nodejs-driver-use.html#additional-connection-options
