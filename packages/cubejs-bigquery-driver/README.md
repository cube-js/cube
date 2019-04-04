# Cube.js BigQuery Database Driver

Pure Javascript BigQuery driver.

[Learn more](https://github.com/statsbotco/cube.js#getting-started)

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

### License

Cube.js BigQuery Database Driver is [Apache 2.0 licensed](./LICENSE).
