<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://docs.cube.dev) • [Blog](https://cube.dev/blog) • [Slack](https://slack.cube.dev) • [Twitter](https://twitter.com/the_cube_dev)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)
[![GitHub Actions](https://github.com/cube-js/cube.js/workflows/Build/badge.svg)](https://github.com/cube-js/cube.js/actions?query=workflow%3ABuild+branch%3Amaster)

# Cube.js BigQuery Database Driver

Pure Javascript BigQuery driver.

[Learn more](https://github.com/cube-js/cube.js#getting-started)

## Credentials

```
CUBEJS_DB_BQ_PROJECT_ID=gcp-project-id
CUBEJS_DB_BQ_KEY_FILE=/path/to/key-file.json
```

Or get base64 version of your key file json using

```
$ cat /path/to/key-file.json | base64
```

And then put base64 string in .env:

```
CUBEJS_DB_BQ_PROJECT_ID=gcp-project-id
CUBEJS_DB_BQ_CREDENTIALS=<base_64_credentials_json>
```

## Optional Parquet pre-aggregation exports

CSV with signed download URLs remains the default. To opt into Parquet, configure
`exportBucketFormat: 'parquet'` in the `BigQueryDriver` constructor along with the
existing `exportBucket` setting. This requires a CubeStore build with external
Parquet import support and a matching driver/orchestrator that advertises and
passes `parquetImport`. Other external drivers continue to receive CSV.

```js
const { BigQueryDriver } = require('@cubejs-backend/bigquery-driver');

module.exports = {
  driverFactory: () => new BigQueryDriver({
    projectId: 'your-project',
    exportBucket: 'your-export-bucket',
    exportBucketFormat: 'parquet',
  }),
};
```

Parquet export returns `gs://` locations. CubeStore's own runtime identity must
have read access to the export bucket, independently of the BigQuery driver's
identity. The CubeStore GCS importer accepts its `CUBESTORE_GCP_KEY_FILE` and
`CUBESTORE_GCP_CREDENTIALS` settings, supported ADC files, local gcloud credentials,
and GKE metadata-server credentials. `object_store` 0.11.1 does not support
arbitrary `external_account` credential files.

Each export attempt uses a unique object prefix. Successful imports release only
that attempt's files. Configure a bucket lifecycle rule for abandoned export
objects from failed or cancelled attempts. There is no deletion of shared table
prefixes before an export.

External Parquet import matches columns by name and converts supported scalar
Arrow types to the destination types. Missing or duplicate requested columns,
failed casts, nested values, and HyperLogLog columns are rejected rather than
silently replaced with nulls. Existing destination precision/scale limits apply;
Parquet does not increase CubeStore's decimal range. CSV-specific import options
are not applicable to Parquet.

### License

Cube.js BigQuery Database Driver is [Apache 2.0 licensed](./LICENSE).
