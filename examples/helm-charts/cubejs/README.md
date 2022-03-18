# Cubejs Chart

## Installing the Chart

```bash
$ cd examples/helm-charts
$ helm install my-release \
--set database.type=<db-type> \
--set ... \
./cubejs
```

## Uninstalling the Chart

To uninstall/delete the `my-release` deployment:

```bash
$ helm delete my-release
```

## Setup

By default a router and one worker will be deployed. You can customize the deployment using helm values.

Refer to the official documentation for more information:
https://cube.dev/docs/reference/environment-variables

### Injecting schema

To inject your schema files in the deployment you have to use `config.volumes` and `config.volumeMounts` values.

Mount path is `/cube/conf/schema` by default and can be customized with the `config.schemaPath` value.

A good practice is to use a ConfigMap to store your all the cube definition files:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: cube-schema
data:
  Cube1.js: |
    cube(`Cube1`, {
      sql: `SELECT * FROM cube1_data`,

      measures: {
        count: {
          type: `count`,
        },
      },
    });
  Cube2.js: |
    cube(`Cube2`, {
      sql: `SELECT * FROM cube2_data`,

      measures: {
        count: {
          type: `count`,
        },
      },
    });
```

### Injecting javascript config

To inject a javascript config in the deployment you can use `config.volumes` and `config.volumeMounts` values.

Mount path is `/cube/conf/`

### Production Example

Deployment with:

- 2 workers
- BigQuery db with exportBucket on GCS
- Schema located in a `cube-schema` ConfigMap
- Redis (using pasword in a secret)
- Cubestore

```bash
$ helm install my-release \
# Set two workers (default 1)
--set workers.workerCount=2 \
# Mount schema volume from ConfigMap
--set config.volumes[0].name=schema \
--set config.volumes[0].configMap.name=cube-schema \
--set config.volumeMounts[0].name=schema \
--set config.volumeMounts[0].readOnly=true \
--set config.volumeMounts[0].mountPath=/cube/conf/schema \
# Database configuration using secret
--set database.type=bigquery \
--set database.bigquery.projectId=<project-id> \
--set database.bigquery.credentialsFromSecret.name=<service-account-secret-name> \
--set database.bigquery.credentialsFromSecret.key=<service-account-secret-key> \
# External Bucket configuration
--set exportBucket.type=gcp \
--set exportBucket.name.key=<bucket-name> \
--set exportBucket.gcsCredentialsFromSecret.name=<service-account-secret-name> \
--set exportBucket.gcsCredentialsFromSecret.name=<service-account-secret-key> \
# Redis configuration
--set redis.url=<redis-url> \
--set redis.passwordFromSecret.name=<redis-secret-name> \
--set redis.passwordFromSecret.key=<redis-secret-key> \
# Cubestore configuration
--set cubestore.host=<cubestore-host> \
./cubejs
```

Or for more readability, using a custom `values.yaml` file:

```bash
$ helm install my-release -f path/to/values.yaml ./cubejs
```

```yaml
# path/to/values.yaml
config:
  volumes:
    - name: schema
      configMap:
        name: cube-schema
  volumeMounts:
    - name: schema
      readOnly: true
      mountPath: /cube/conf/schema

workers:
  workersCount: 2

redis:
  url: <redis-url>
  passwordFromSecret:
    name: <redis-secret-name>
    key: <redis-secret-key>

database:
  type: bigquery
  bigquery:
    projectId: <project-id>
    credentialsFromSecret:
      name: <service-account-secret-name>
      key: <service-account-secret-key>

exportBucket:
  type: gcp
  name: <bucket-name>
  gcsCredentialsFromSecret:
    name: <service-account-secret-name>
    key: <service-account-secret-key>

cubestore:
  host: <cubestore-host>
```

## Parameters

### Common parameters

| Name                | Description                                                  | Value |
| ------------------- | ------------------------------------------------------------ | ----- |
| `nameOverride`      | Override the name                                            | `""`  |
| `fullnameOverride`  | Provide a name to substitute for the full names of resources | `""`  |
| `commonLabels`      | Labels to add to all deployed objects                        | `{}`  |
| `commonAnnotations` | Annotations to add to all deployed objects                   | `{}`  |

### Image parameters

| Name               | Description                                          | Value          |
| ------------------ | ---------------------------------------------------- | -------------- |
| `image.repository` | Cubestore image repository                           | `cubejs/cube`  |
| `image.tag`        | Cubestore image tag (immutable tags are recommended) | `0.28.26`      |
| `image.pullPolicy` | Cubestore image pull policy                          | `IfNotPresent` |

### Config parameters

| Name                                | Description                                                                                                                     | Value   |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `config.apiPort`                    | The port for a Cube.js deployment to listen to API connections on                                                               | `4000`  |
| `config.sqlPort`                    | The port for a Cube.js deployment to listen to SQL connections on                                                               |         |
| `config.sqlUser`                    | The username to access the SQL api                                                                                              |         |
| `config.sqlPassword`                | The password to access the SQL api                                                                                              |         |
| `config.sqlPasswordFromSecret.name` | The password to access the SQL api (using secret)                                                                               |         |
| `config.sqlPasswordFromSecret.key`  | The password to access the SQL api (using secret)                                                                               |         |
| `config.devMode`                    | If true, enables development mode                                                                                               | `false` |
| `config.debug`                      | If true, enables debug logging                                                                                                  | `false` |
| `config.logLevel`                   | The logging level for Cube.js                                                                                                   | `warn`  |
| `config.externalDefault`            | If true, uses Cube Store or an external database for storing Pre-aggregations                                                   | `true`  |
| `config.telemetry`                  | If true, then send telemetry to CubeJS                                                                                          | `false` |
| `config.apiSecret`                  | The secret key used to sign and verify JWTs. Generated on project scaffold                                                      |         |
| `config.apiSecretFromSecret.name`   | The secret key used to sign and verify JWTs. Generated on project scaffold (using secret)                                       |         |
| `config.apiSecretFromSecret.key`    | The secret key used to sign and verify JWTs. Generated on project scaffold (using secret)                                       |         |
| `config.schemaPath`                 | The path where Cube.js loads schemas from. Defaults to schema                                                                   |         |
| `config.app`                        | An application ID used to uniquely identify the Cube.js deployment. Can be different for multitenant setups. Defaults to cubejs |         |
| `config.rollupOnly`                 | If true, this instance of Cube.js will only query rollup pre-aggregations. Defaults to false                                    |         |
| `config.scheduledRefreshTimezones`  | A comma-separated list of timezones to schedule refreshes for                                                                   |         |
| `config.webSockets`                 | If true, then use WebSocket for data fetching. Defaults to true                                                                 |         |
| `config.preAggregationsSchema`      | The schema name to use for storing pre-aggregations true                                                                        |         |
| `config.cacheAndQueueDriver`        | The cache and queue driver to use for the Cube.js deployment. Defaults to redis                                                 |         |
| `config.topicName`                  | The name of the Amazon SNS or Google Cloud Pub/Sub topicredis                                                                   |         |
| `config.volumes`                    | The config volumes. Will be used to both master and workers                                                                     | `[]`    |
| `config.volumeMounts`               | The config volumeMounts. Will be used to both master and workers                                                                | `[]`    |

### Redis parameters

| Name                            | Description                                                                                                                                              | Value |
| ------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- | ----- |
| `redis.url`                     | The host URL for a Redis server                                                                                                                          |       |
| `redis.password`                | The password used to connect to the Redis server                                                                                                         |       |
| `redis.passwordFromSecret.name` | The password used to connect to the Redis server (using secret)                                                                                          |       |
| `redis.passwordFromSecret.key`  | The password used to connect to the Redis server (using secret)                                                                                          |       |
| `redis.tls`                     | If true, then the connection to the Redis server is protected by TLS authentication. Defaults to false                                                   |       |
| `redis.poolMin`                 | The minimum number of connections to keep active in the Redis connection pool for a single appId (tenant). Must be lower than poolMax. Defaults to 2     |       |
| `redis.poolMax`                 | The maximum number of connections to keep active in the Redis connection pool for a single appId (tenant). Must be higher than poolMin. Defaults to 1000 |       |
| `redis.useIoRedis`              | Use ioredis instead of redis. Defaults to false                                                                                                          |       |

### JWT parameters

| Name                      | Description                                                                               | Value |
| ------------------------- | ----------------------------------------------------------------------------------------- | ----- |
| `jwt.url`                 | A valid URL to a JSON Web Key Sets (JWKS)                                                 |       |
| `jwt.key`                 | The secret key used to sign and verify JWTs. Generated on project scaffold                |       |
| `jwt.keyFromSecret.name`  | The secret key used to sign and verify JWTs. Generated on project scaffold (using secret) |       |
| `jwt.keyFromSecret.value` | The secret key used to sign and verify JWTs. Generated on project scaffold (using secret) |       |
| `jwt.audience`            | An audience value which will be used to enforce the aud claim from inbound JWTs           |       |
| `jwt.issuer`              | An issuer value which will be used to enforce the iss claim from inbound JWTs             |       |
| `jwt.subject`             | A subject value which will be used to enforce the sub claim from inbound JWTs             |       |
| `jwt.algs`                | Any supported algorithm for decoding JWTs                                                 |       |
| `jwt.claimsNamespace`     | A namespace within the decoded JWT under which any custom claims can be found             |       |

### Database parameters

| Name                                           | Description                                                                      | Value   |
| ---------------------------------------------- | -------------------------------------------------------------------------------- | ------- |
| `database.type`                                | A database type supported by Cube.js                                             |         |
| `database.url`                                 | The URL for a database                                                           |         |
| `database.host`                                | The host URL for a database                                                      |         |
| `database.port`                                | The port for the database connection                                             |         |
| `database.schema`                              | The schema within the database to connect to                                     |         |
| `database.name`                                | The name of the database to connect to                                           |         |
| `database.user`                                | The username used to connect to the database                                     |         |
| `database.pass`                                | The password used to connect to the database                                     |         |
| `database.passFromSecret.name`                 | The password used to connect to the database (using secret)                      |         |
| `database.passFromSecret.key`                  | The password used to connect to the database (using secret)                      |         |
| `database.domain`                              | A domain name within the database to connect to                                  |         |
| `database.socketPath`                          | The path to a Unix socket for a MySQL database                                   |         |
| `database.catalog`                             | The catalog within the database to connect to                                    |         |
| `database.maxPool`                             | The maximum number of connections to keep active in the database connection pool |         |
| `database.ssl.enabled`                         | If true, enables SSL encryption for database connections from Cube.js            | `false` |
| `database.ssl.rejectUnAuthorized`              | If true, verifies the CA chain with the system's built-in CA chain               |         |
| `database.ssl.ca`                              | The contents of a CA bundle in PEM format, or a path to one                      |         |
| `database.ssl.cert`                            | The contents of an SSL certificate in PEM format, or a path to one               |         |
| `database.ssl.key`                             | The contents of a private key in PEM format, or a path to one                    |         |
| `database.ssl.ciphers`                         | The ciphers used by the SSL certificate                                          |         |
| `database.ssl.serverName`                      | The server name for the SNI TLS extension                                        |         |
| `database.ssl.passPhrase`                      | he passphrase used to encrypt the SSL private key                                |         |
| `database.aws.key`                             | The AWS Access Key ID to use for database connections                            |         |
| `database.aws.keyFromSecret.name`              | The AWS Access Key ID to use for database connections (using secret)             |         |
| `database.aws.keyFromSecret.key`               | The AWS Access Key ID to use for database connections (using secret)             |         |
| `database.aws.region`                          | The AWS region of the Cube.js deployment                                         |         |
| `database.aws.s3OutputLocation`                | The S3 path to store query results made by the Cube.js deployment                |         |
| `database.aws.secret`                          | The AWS Secret Access Key to use for database connections                        |         |
| `database.aws.secretFromSecret.name`           | The AWS Secret Access Key to use for database connections (using secret)         |         |
| `database.aws.secretFromSecret.key`            | The AWS Secret Access Key to use for database connections (using secret)         |         |
| `database.aws.athenaWorkgroup`                 | The name of the workgroup in which the query is being started                    |         |
| `database.bigquery.projectId`                  | The Google BigQuery project ID to connect to                                     |         |
| `database.bigquery.location`                   | The Google BigQuery dataset location to connect to                               |         |
| `database.bigquery.credentials`                | A Base64 encoded JSON key file for connecting to Google BigQuery                 |         |
| `database.bigquery.credentialsFromSecret.name` | A Base64 encoded JSON key file for connecting to Google BigQuery (using secret)  |         |
| `database.bigquery.credentialsFromSecret.key`  | A Base64 encoded JSON key file for connecting to Google BigQuery (using secret)  |         |
| `database.hive.cdhVersion`                     | The version of the CDH instance for Apache Hive                                  |         |
| `database.hive.thriftVersion`                  | The version of Thrift Server for Apache Hive                                     |         |
| `database.hive.type`                           | The type of Apache Hive server                                                   |         |
| `database.hive.version`                        | The version of Apache Hive                                                       |         |
| `database.jdbc.driver`                         | The driver of jdbc connection                                                    |         |
| `database.jdbc.url`                            | The URL for a JDBC connection                                                    |         |
| `database.snowFlake.account`                   | The Snowflake account ID to use when connecting to the database                  |         |
| `database.snowFlake.region`                    | The Snowflake region to use when connecting to the database                      |         |
| `database.snowFlake.role`                      | The Snowflake role to use when connecting to the database                        |         |
| `database.snowFlake.warehouse`                 | The Snowflake warehouse to use when connecting to the database                   |         |
| `database.snowFlake.clientSessionKeepAlive`    | If true, keep the Snowflake connection alive indefinitely                        |         |
| `database.snowFlake.authenticator`             | The type of authenticator to use with Snowflake. Defaults to SNOWFLAKE           |         |
| `database.snowFlake.privateKeyPath`            | The path to the private RSA key folder                                           |         |
| `database.snowFlake.privateKeyPass`            | The password for the private RSA key. Only required for encrypted keys           |         |
| `database.databricks.url`                      | The URL for a JDBC connection                                                    |         |

### External Database parameters

| Name                                   | Description                                                                           | Value |
| -------------------------------------- | ------------------------------------------------------------------------------------- | ----- |
| `externalDatabase.type`                | Alternative to Cube Store storage for pre-aggregations                                |       |
| `externalDatabase.host`                | The host URL for an external pre-aggregations database                                |       |
| `externalDatabase.port`                | The port for the external pre-aggregations database                                   |       |
| `externalDatabase.name`                | The name of the external pre-aggregations database to connect to                      |       |
| `externalDatabase.pass`                | Base64 encoded JSON key file for connecting to Google Cloud                           |       |
| `externalDatabase.passFromSecret.name` | The password used to connect to the external pre-aggregations database (using secret) |       |
| `externalDatabase.passFromSecret.key`  | The password used to connect to the external pre-aggregations database (using secret) |       |
| `externalDatabase.user`                | The username used to connect to the external pre-aggregations database                |       |

### Export Bucket parameters

| Name                                         | Description                                                                | Value |
| -------------------------------------------- | -------------------------------------------------------------------------- | ----- |
| `exportBucket.name`                          | The name of a bucket in cloud storage                                      |       |
| `exportBucket.type`                          | The cloud provider where the bucket is hosted (gcs, s3)                    |       |
| `exportBucket.gcsCredentials`                | Base64 encoded JSON key file for connecting to Google Cloud                |       |
| `exportBucket.gcsCredentialsFromSecret.name` | Base64 encoded JSON key file for connecting to Google Cloud (using secret) |       |
| `exportBucket.gcsCredentialsFromSecret.key`  | Base64 encoded JSON key file for connecting to Google Cloud (using secret) |       |

### Cubestore parameters

| Name             | Description                               | Value |
| ---------------- | ----------------------------------------- | ----- |
| `cubestore.host` | The hostname of the Cube Store deployment |       |
| `cubestore.port` | The port of the Cube Store deployment     |       |

### Master parameters

| Name                                        | Description                                          | Value  |
| ------------------------------------------- | ---------------------------------------------------- | ------ |
| `master.affinity`                           | Affinity for pod assignment                          | `{}`   |
| `master.spreadConstraints`                  | Topology spread constraint for pod assignment        | `[]`   |
| `master.resources`                          | Define resources requests and limits for single Pods | `{}`   |
| `master.livenessProbe.enabled`              | Enable livenessProbe                                 | `true` |
| `master.livenessProbe.initialDelaySeconds`  | Initial delay seconds for livenessProbe              | `10`   |
| `master.livenessProbe.periodSeconds`        | Period seconds for livenessProbe                     | `30`   |
| `master.livenessProbe.timeoutSeconds`       | Timeout seconds for livenessProbe                    | `3`    |
| `master.livenessProbe.successThreshold`     | Failure threshold for livenessProbe                  | `1`    |
| `master.livenessProbe.failureThreshold`     | Success threshold for livenessProbe                  | `3`    |
| `master.readinessProbe.enabled`             | Enable readinessProbe                                | `true` |
| `master.readinessProbe.initialDelaySeconds` | Initial delay seconds for readinessProbe             | `10`   |
| `master.readinessProbe.periodSeconds`       | Period seconds for readinessProbe                    | `30`   |
| `master.readinessProbe.timeoutSeconds`      | Timeout seconds for readinessProbe                   | `3`    |
| `master.readinessProbe.successThreshold`    | Failure threshold for readinessProbe                 | `1`    |
| `master.readinessProbe.failureThreshold`    | Success threshold for readinessProbe                 | `3`    |
| `master.customLivenessProbe`                | Custom livenessProbe that overrides the default one  | `{}`   |
| `master.customReadinessProbe`               | Custom readinessProbe that overrides the default one | `{}`   |

### Workers parameters

| Name                                         | Description                                          | Value  |
| -------------------------------------------- | ---------------------------------------------------- | ------ |
| `workers.workersCount`                       | Number of workers to deploy                          | `1`    |
| `workers.affinity`                           | Affinity for pod assignment                          | `{}`   |
| `workers.spreadConstraints`                  | Topology spread constraint for pod assignment        | `[]`   |
| `workers.resources`                          | Define resources requests and limits for single Pods | `{}`   |
| `workers.livenessProbe.enabled`              | Enable livenessProbe                                 | `true` |
| `workers.livenessProbe.initialDelaySeconds`  | Initial delay seconds for livenessProbe              | `10`   |
| `workers.livenessProbe.periodSeconds`        | Period seconds for livenessProbe                     | `30`   |
| `workers.livenessProbe.timeoutSeconds`       | Timeout seconds for livenessProbe                    | `3`    |
| `workers.livenessProbe.successThreshold`     | Failure threshold for livenessProbe                  | `1`    |
| `workers.livenessProbe.failureThreshold`     | Success threshold for livenessProbe                  | `3`    |
| `workers.readinessProbe.enabled`             | Enable readinessProbe                                | `true` |
| `workers.readinessProbe.initialDelaySeconds` | Initial delay seconds for readinessProbe             | `10`   |
| `workers.readinessProbe.periodSeconds`       | Period seconds for readinessProbe                    | `30`   |
| `workers.readinessProbe.timeoutSeconds`      | Timeout seconds for readinessProbe                   | `3`    |
| `workers.readinessProbe.successThreshold`    | Failure threshold for readinessProbe                 | `1`    |
| `workers.readinessProbe.failureThreshold`    | Success threshold for readinessProbe                 | `3`    |
| `workers.customLivenessProbe`                | Custom livenessProbe that overrides the default one  | `{}`   |
| `workers.customReadinessProbe`               | Custom readinessProbe that overrides the default one | `{}`   |

## Ingress parameters

| Name                       | Description                                                                     | Value                    |
| -------------------------- | ------------------------------------------------------------------------------- | ------------------------ |
| `ingress.enabled`          | Set to true to enable ingress record generation                                 | `false`                  |
| `ingress.hostname`         | When the ingress is enabled, a host pointing to this will be created            | `cubejs.local`           |
| `ingress.path`             | The Path to Cubejs                                                              | `/`                      |
| `ingress.pathPrefix`       | The PathPrefix                                                                  | `ImplementationSpecific` |
| `ingress.ingressClassName` | The Ingress class name                                                          |                          |
| `ingress.annotations`      | Ingress annotations                                                             | `{}`                     |
| `ingress.tls`              | Enable TLS configuration for the hostname defined at ingress.hostname parameter | `false`                  |
