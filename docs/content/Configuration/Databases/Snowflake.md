---
title: Snowflake
permalink: /config/databases/snowflake
---

## Prerequisites

- [The account ID][dbt-docs-snowflake-account] for [Snowflake][snowflake]
- The warehouse name in the [Snowflake][snowflake] account
- [The region][snowflake-docs-regions] for the [Snowflake][snowflake] warehouse
- The username/password for the [Snowflake][snowflake] account

## Setup

### Manual

Add the following to a `.env` file in your Cube.js project:

```bash
CUBEJS_DB_TYPE=snowflake
CUBEJS_DB_SNOWFLAKE_ACCOUNT=XXXXXXXXX.us-east-1
CUBEJS_DB_SNOWFLAKE_REGION=us-east-1
CUBEJS_DB_SNOWFLAKE_WAREHOUSE=MY_SNOWFLAKE_WAREHOUSE
CUBEJS_DB_NAME=my_snowflake_database
CUBEJS_DB_USER=snowflake_user
CUBEJS_DB_PASS=**********
```

## Environment Variables

| Environment Variable                            | Description                                                                                                                                         | Possible Values                                            | Required |
| ----------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------- | :------: |
| `CUBEJS_DB_SNOWFLAKE_ACCOUNT`                   | The Snowflake account ID to use when connecting to the database                                                                                     | [A valid Snowflake account ID][dbt-docs-snowflake-account] |    ✅    |
| `CUBEJS_DB_SNOWFLAKE_REGION`                    | The Snowflake region to use when connecting to the database                                                                                         | [A valid Snowflake region][snowflake-docs-regions]         |    ✅    |
| `CUBEJS_DB_SNOWFLAKE_WAREHOUSE`                 | The Snowflake warehouse to use when connecting to the database                                                                                      | A valid Snowflake warehouse for the account                |    ✅    |
| `CUBEJS_DB_SNOWFLAKE_ROLE`                      | The Snowflake role to use when connecting to the database                                                                                           | A valid Snowflake role for the account                     |    ❌    |
| `CUBEJS_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE` | If `true`, [keep the Snowflake connection alive indefinitely][snowflake-docs-connection-options]                                                    | `true`, `false`                                            |    ❌    |
| `CUBEJS_DB_NAME`                                | The name of the database to connect to                                                                                                              | A valid database name                                      |    ✅    |
| `CUBEJS_DB_USER`                                | The username used to connect to the database                                                                                                        | A valid database username                                  |    ✅    |
| `CUBEJS_DB_PASS`                                | The password used to connect to the database                                                                                                        | A valid database password                                  |    ✅    |
| `CUBEJS_DB_SNOWFLAKE_AUTHENTICATOR`             | The type of authenticator to use with Snowflake. Use `SNOWFLAKE` with username/password, or `SNOWFLAKE_JWT` with key pairs. Defaults to `SNOWFLAKE` | `SNOWFLAKE`, `SNOWFLAKE_JWT`                               |    ❌    |
| `CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PATH`          | The path to the private RSA key folder                                                                                                              | A valid path to the private RSA key                        |    ❌    |
| `CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PASS`          | The password for the private RSA key. Only required for encrypted keys                                                                              | A valid password for the encrypted private RSA key         |    ❌    |

## SSL

Cube.js does not require any additional configuration to enable SSL as Snowflake
connections are made over HTTPS.

## Export bucket

Snowflake supports using both AWS S3 and Google Cloud Storage for export bucket
functionality.

### AWS S3

<!-- prettier-ignore-start -->
[[info |]]
| Ensure the AWS credentials are correctly configured in IAM to allow reads and
| writes to the export bucket in S3.
<!-- prettier-ignore-end -->

```dotenv
CUBEJS_DB_EXPORT_BUCKET_TYPE=s3
CUBEJS_DB_EXPORT_BUCKET=my.bucket.on.s3
CUBEJS_DB_EXPORT_BUCKET_AWS_KEY=<AWS_KEY>
CUBEJS_DB_EXPORT_BUCKET_AWS_SECRET=<AWS_SECRET>
CUBEJS_DB_EXPORT_BUCKET_AWS_REGION=<AWS_REGION>
```

### Google Cloud Storage

<!-- prettier-ignore-start -->
[[info |]]
| When using an export bucket, remember to assign the **Storage Object Admin**
| role to your BigQuery credentials (`CUBEJS_DB_EXPORT_GCS_CREDENTIALS`).
<!-- prettier-ignore-end -->

Before configuring Cube.js, an [integration must be created and configured in
Snowflake][snowflake-docs-gcs-integration]. Take note of the integration name
(`gcs_int` from the example link) as you'll need it to configure Cube.js.

Once the Snowflake integration is set up, configure Cube.js using the following:

```dotenv
CUBEJS_DB_EXPORT_BUCKET=snowflake-export-bucket
CUBEJS_DB_EXPORT_BUCKET_TYPE=gcp
CUBEJS_DB_EXPORT_GCS_CREDENTIALS=<BASE64_ENCODED_SERVICE_CREDENTIALS_JSON>
CUBEJS_DB_EXPORT_INTEGRATION=gcs_int
```

[dbt-docs-snowflake-account]:
  https://docs.getdbt.com/reference/warehouse-profiles/snowflake-profile#account
[ref-caching-large-preaggs]: /using-pre-aggregations#large-pre-aggregations
[ref-env-var]: /reference/environment-variables#database-connection
[snowflake]: https://www.snowflake.com/
[snowflake-docs-connection-options]:
  https://docs.snowflake.com/en/user-guide/nodejs-driver-use.html#additional-connection-options
[snowflake-docs-gcs-integration]:
  https://docs.snowflake.com/en/user-guide/data-load-gcs-config.html
[snowflake-docs-regions]:
  https://docs.snowflake.com/en/user-guide/intro-regions.html
