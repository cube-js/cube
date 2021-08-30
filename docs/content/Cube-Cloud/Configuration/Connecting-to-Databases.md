---
title: Connecting to the Database
permalink: /cloud/configuration/connecting-to-the-database
category: Configuration
menuOrder: 1
redirect_from:
  - /cloud/configuration/connecting-to-databases
---

You can connect all Cube.js supported databases to your Cube Cloud deployment.

<div
  style="text-align: center"
>
  <img
  alt="Cube Cloud Supported Databases Screen"
  src="https://cube.dev/downloads/images/cube-cloud-databases-list.png"
  style="border: none"
  width="100%"
  />
</div>

Below you can find guides on how to use Cube Cloud with specific database
vendors.

- [AWS Athena](#guides-aws-athena)
- [AWS Redshift](#guides-aws-redshift)
- [BigQuery](#guides-big-query)
- [Snowflake](#guides-snowflake)

## Guides

### AWS Athena

The following fields are required when creating an AWS Athena connection:

| Field                     | Description                                                       | Examples                                   |
| ------------------------- | ----------------------------------------------------------------- | ------------------------------------------ |
| **AWS Access Key ID**     | The AWS Access Key ID to use for database connections             | `AKIAXXXXXXXXXXXXXXXX`                     |
| **AWS Secret Access Key** | The AWS Secret Access Key to use for database connections         | `asd+/Ead123456asc23ASD2Acsf23/1A3fAc56af` |
| **AWS Region**            | The AWS region of the Cube.js deployment                          | `us-east-1`                                |
| **S3 Output Location**    | The S3 path to store query results made by the Cube.js deployment | `s3://my-output-bucket/outputs/`           |

<div
  style="text-align: center"
>
  <img
  alt="Cube Cloud AWS Athena Configuration Screen"
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Cube-Cloud/Configuration/connect-db-athena.png"
  style="border: none"
  width="100%"
  />
</div>

### AWS Redshift

<!-- prettier-ignore-start -->
[[warning |]]
| Ensure that the database can be accessed over the public Internet. If you'd
| prefer to keep the database on a private network, [contact us for VPC peering
| solutions](#connecting-to-a-database-not-exposed-over-the-internet).
<!-- prettier-ignore-end -->

The following fields are required when creating an AWS Redshift connection:

| Field        | Description                                  | Examples                                                       |
| ------------ | -------------------------------------------- | -------------------------------------------------------------- |
| **Hostname** | The host URL for the AWS Redshift cluster    | `examplecluster.abc123xyz789.us-west-2.redshift.amazonaws.com` |
| **Port**     | The port for the AWS Redshift cluster        | `5439`                                                         |
| **Database** | The name of the database to connect to       | `public`                                                       |
| **Username** | The username used to connect to the database | `redshift`                                                     |
| **Password** | The password used to connect to the database | `MY_SUPER_SECRET_PASSWORD`                                     |

<div
  style="text-align: center"
>
  <img
  alt="Cube Cloud AWS Redshift Configuration Screen"
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Cube-Cloud/Configuration/connect-db-redshift.png"
  style="border: none"
  width="100%"
  />
</div>

### BigQuery

The following fields are required when creating a BigQuery connection:

| Field                         | Description                                                                                                             | Examples                                                         |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------- |
| **Service Account JSON file** | A JSON key file for connecting to Google BigQuery                                                                       | A valid Google BigQuery JSON key file                            |
| **Project ID**                | The Google BigQuery project ID to connect to                                                                            | `my-bigquery-project`                                            |
| **Encoded Key File**          | A Base64 encoded JSON key file for connecting to Google BigQuery. Required if Service Account JSON file is not provided | A valid Google BigQuery JSON key file encoded as a Base64 string |

<div
  style="text-align: center"
>
  <img
  alt="Cube Cloud BigQuery Configuration Screen"
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Cube-Cloud/Configuration/connect-db-bigquery.png"
  style="border: none"
  width="100%"
  />
</div>

### Snowflake

The following fields are required when creating a Snowflake connection:

| Field         | Description                                                     | Examples                   |
| ------------- | --------------------------------------------------------------- | -------------------------- |
| **Username**  | The username used to connect to the database                    | `cube`                     |
| **Password**  | The password used to connect to the database                    | `MY_SUPER_SECRET_PASSWORD` |
| **Database**  | The name of the database to connect to                          | `MY_SNOWFLAKE_DB`          |
| **Account**   | The Snowflake account ID to use when connecting to the database | `qna90001`                 |
| **Region**    | The Snowflake region to use when connecting to the database     | `us-east-1`                |
| **Warehouse** | The Snowflake warehouse to use when connecting to the database  | `MY_WAREHOUSE`             |
| **Role**      | The Snowflake role to use when connecting to the database       | `PUBLIC`                   |

<div
  style="text-align: center"
>
  <img
  alt="Cube Cloud Snowflake Configuration Screen"
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Cube-Cloud/Configuration/connect-db-snowflake.png"
  style="border: none"
  width="100%"
  />
</div>

## Connecting to multiple databases

If you are connecting to multiple databases you can skip the database connection
step during the deployment creation. First, make sure you have the correct
configuration in your `cube.js` file according to your
[multitenancy setup](/multitenancy-setup). Next, configure the corresponding
environment variables on the **Settings - Env Vars page**.

## Connecting via SSL

When setting up a new deployment, simply select the SSL checkbox when entering
database credentials:

<div
  style="text-align: center"
>
  <img
  alt="Cube Cloud Database Connection Screen"
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Cube-Cloud/Configuration/ssl-wizard.png"
  style="border: none"
  width="100%"
  />
</div>

### Custom SSL certificates

To use custom SSL certificates between Cube Cloud and your database server, go
to the **Env vars** tab in **Settings**:

<!-- prettier-ignore-start -->
[[warning]]
| Depending on how SSL is configured on your database server, you may need to
| specify additional environment variables, please check the [Environment
| Variables reference][ref-config-env-vars] for more information.
<!-- prettier-ignore-end -->

<div
  style="text-align: center"
>
  <img
  alt="Cube Cloud Database Connection Screen"
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Cube-Cloud/Configuration/ssl-custom.png"
  style="border: none"
  width="100%"
  />
</div>

Add the following environment variables:

| Environment Variable | Description                                                                                                                                                                                                 | Example                                  |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------- |
| `CUBEJS_DB_SSL`      | If `true`, enables SSL encryption for database connections from Cube.js                                                                                                                                     | `true`, `false`                          |
| `CUBEJS_DB_SSL_CA`   | The contents of a CA bundle in PEM format, or a path to one. For more information, check the `options.ca` property for TLS Secure Contexts [in the Node.js documentation][link-nodejs-tls-options]          | A valid CA bundle or a path to one       |
| `CUBEJS_DB_SSL_CERT` | The contents of an SSL certificate in PEM format, or a path to one. For more information, check the `options.cert` property for TLS Secure Contexts [in the Node.js documentation][link-nodejs-tls-options] | A valid SSL certificate or a path to one |
| `CUBEJS_DB_SSL_KEY`  | The contents of a private key in PEM format, or a path to one. For more information, check the `options.key` property for TLS Secure Contexts [in the Node.js documentation][link-nodejs-tls-options]       | A valid SSL private key or a path to one |

## Allowing connections from Cube Cloud IP

In some cases you'd need to allow connections from your Cube Cloud deployment IP
address to your database. You can copy the IP address from either the Database
Setup step in deployment creation, or from the Env Vars tab in your deployment
Settings page.

## Connecting to a database not exposed over the internet

[Contact us](mailto:support@cube.dev) for VPC peering and on-premise solutions.

[link-aws-regions]:
  https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-available-regions
[link-nodejs-tls-options]:
  https://nodejs.org/docs/latest/api/tls.html#tls_tls_createsecurecontext_options
[link-snowflake-account]:
  https://docs.getdbt.com/reference/warehouse-profiles/snowflake-profile#account
[link-snowflake-regions]:
  https://docs.snowflake.com/en/user-guide/intro-regions.html
[ref-config-env-vars]: /reference/environment-variables#database-connection
