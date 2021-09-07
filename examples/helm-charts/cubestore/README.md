# Cubestore Chart

## Installing the Chart

```bash
$ cd examples/helm-charts
$ helm install my-release ./cubestore
```

## Uninstalling the Chart

To uninstall/delete the `my-release` deployment:

```bash
$ helm delete my-release
```

## Customize values

By default a router and two workers will be deployed. You can customize the deployment using helm values.

Refer to the official documentation for more information:
https://cube.dev/docs/reference/environment-variables#cube-store

### Example

Deployment with:

- 3 workers
- GCP cloud storage (using a secret)

```bash
$ helm install my-release \
--set workers.workersCount=3 \
--set cloudStorage.gcp.credentialsFromSecret.name=<service-account-secret-name> \
--set cloudStorage.gcp.credentialsFromSecret.key=<service-account-secret-key> \
--set cloudStorage.gcp.bucket=<my-bucket>
./cubestore
```

## Persistance

### Remote dir

By default a shared remoteDir is created to store metadata and datasets if no cloudstorage is configured.
Prefer usin cloudStorage if your are running on `gcp` or `aws`.

### Local dir

By default local dir are not persisted. You can enable persistance on router and master.

## Parameters

### Common parameters

| Name                | Description                                                  | Value |
| ------------------- | ------------------------------------------------------------ | ----- |
| `nameOverride`      | Override the name                                            | `""`  |
| `fullnameOverride`  | Provide a name to substitute for the full names of resources | `""`  |
| `commonLabels`      | Labels to add to all deployed objects                        | `{}`  |
| `commonAnnotations` | Annotations to add to all deployed objects                   | `{}`  |

### Image parameters

| Name               | Description                                          | Value              |
| ------------------ | ---------------------------------------------------- | ------------------ |
| `image.repository` | Cubestore image repository                           | `cubejs/cubestore` |
| `image.tag`        | Cubestore image tag (immutable tags are recommended) | `0.28.26`          |
| `image.pullPolicy` | Cubestore image pull policy                          | `IfNotPresent`     |

### Global parameters

| Name                       | Description                                                                                                       | Value   |
| -------------------------- | ----------------------------------------------------------------------------------------------------------------- | ------- |
| `config.logLevel`          | The logging level for Cube Store                                                                                  | `error` |
| `config.noUpload`          | If true, prevents uploading serialized pre-aggregations to cloud storage                                          |         |
| `config.jobRunners`        | The number of parallel tasks that process non-interactive jobs like data insertion, compaction etc. Defaults to 4 |         |
| `config.queryTimeout`      | The timeout for SQL queries in seconds. Defaults to 120                                                           |         |
| `config.selectWorkers`     | The number of Cube Store sub-processes that handle SELECT queries. Defaults to 4                                  |         |
| `config.walSplitThreshold` | The maximum number of rows to keep in a single chunk of data right after insertion. Defaults to 262144            |         |

### Remote dir parameters

| Name                                   | Description                                                                | Value  |
| -------------------------------------- | -------------------------------------------------------------------------- | ------ |
| `remoteDir.persistence.resourcePolicy` | Setting it to "keep" to avoid removing PVCs during a helm delete operation | `keep` |
| `remoteDir.persistence.size`           | Persistent Volume size                                                     | `10Gi` |
| `remoteDir.persistence.annotations`    | Additional custom annotations for the PVC                                  | `{}`   |

### Cloud Storage parameters

| Name                                          | Description                                                                                                            | Value |
| --------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- | ----- |
| `cloudStorage.gcp.credentials`                | A Base64 encoded JSON key file for connecting to Google Cloud. Required when using Google Cloud Storage                |
| `cloudStorage.gcp.credentialsFromSecret.name` | A Base64 encoded JSON key file for connecting to Google Cloud. Required when using Google Cloud Storage (using secret) |       |
| `cloudStorage.gcp.credentialsFromSecret.key`  | A Base64 encoded JSON key file for connecting to Google Cloud. Required when using Google Cloud Storage (using secret) |       |
| `cloudStorage.gcp.bucket`                     | The name of a bucket in GCS. Required when using GCS                                                                   |       |
| `cloudStorage.gcp.subPath`                    | The path in a GCS bucket to store pre-aggregations. Optional                                                           |       |
| `cloudStorage.aws.accessKeyID`                | The Access Key ID for AWS. Required when using AWS S3                                                                  |       |
| `cloudStorage.aws.secretKey`                  | A Base64 encoded JSON key file for connecting to Google Cloud. Required when using Google Cloud Storage                |       |
| `cloudStorage.aws.secretKeyFromSecret.name`   | The Secret Access Key for AWS. Required when using AWS S3 (using secret)                                               |       |
| `cloudStorage.aws.secretKeyFromSecret.key`    | The Secret Access Key for AWS. Required when using AWS S3 (using secret)                                               |       |
| `cloudStorage.aws.bucket`                     | The name of a bucket in AWS S3. Required when using AWS S3                                                             |       |
| `cloudStorage.aws.region`                     | The region of a bucket in AWS S3. Required when using AWS S3                                                           |       |
| `cloudStorage.aws.subPath`                    | The path in a AWS S3 bucket to store pre-aggregations. Optional                                                        |       |

### Router parameters

| Name                             | Description                                                      | Value             |
| -------------------------------- | ---------------------------------------------------------------- | ----------------- |
| `router.httpPort`                | The port for Cube Store to listen to HTTP connections on         | `3030`            |
| `router.metaPort`                | The port for the router node to listen for connections on        | `9999`            |
| `router.mysqlPort`               | The port for Cube Store to listen to connections on              | `3306`            |
| `router.statusPort`              | The port for Cube Store to expose status probes                  | `3331`            |
| `router.persistence.enabled`     | Enable persistence for local data using Persistent Volume Claims | `false`           |
| `router.persistance.size`        | Persistent Volume size                                           | `10Gi`            |
| `router.persistance.accessModes` | Persistent Volume access modes                                   | [`ReadWriteOnce`] |
| `router.persistance.annotations` | Additional custom annotations for the PVC                        | `{}`              |
| `router.affinity`                | Affinity for pod assignment                                      | `{}`              |
| `router.spreadConstraints`       | Topology spread constraint for pod assignment                    | `[]`              |
| `router.resources`               | Define resources requests and limits for single Pods             | `{}`              |

### Workers parameters

| Name                              | Description                                                      | Value             |
| --------------------------------- | ---------------------------------------------------------------- | ----------------- |
| `workers.workersCount`            | Number of workers to deploy                                      | `2`               |
| `workers.port`                    | The port for the router node to listen for connections on        | `9001`            |
| `workers.persistence.enabled`     | Enable persistence for local data using Persistent Volume Claims | `false`           |
| `workers.persistance.size`        | Persistent Volume size                                           | `10Gi`            |
| `workers.persistance.accessModes` | Persistent Volume access modes                                   | [`ReadWriteOnce`] |
| `workers.persistance.annotations` | Additional custom annotations for the PVC                        | `{}`              |
| `workers.affinity`                | Affinity for pod assignment                                      | `{}`              |
| `workers.spreadConstraints`       | Topology spread constraint for pod assignment                    | `[]`              |
| `workers.resources`               | Define resources requests and limits for single Pods             | `{}`              |
