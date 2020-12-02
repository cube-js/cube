---
title: Command Reference
permalink: /reference
category: Cube.js CLI
menuOrder: 2
---

## create

The `create` command generates barebones Cube.js app.

### Usage

```bash
$ cubejs create APP-NAME -d DB-TYPE [-t TEMPLATE]
```

### Flags

| Parameter | Description | Values |
| --------- | ----------- | ------ |
| -d, --db-type <db-type> | Preconfigure Cube.js app for selected database. | `postgres`, `mysql`, `athena`, `mongobi`, `bigquery`, `redshift`, `mssql`, `clickhouse`, `snowflake`, `presto`, `druid` |
| -t, --template <template> | Framework running Cube.js backend. | `docker` (default), `express`, `serverless`, `serverless-aws` |

### Example

Create app called `demo-app` using default (`docker`) template and `mysql` database:

```bash
$ cubejs create demo-app -d mysql
```

Create app called `demo-app` using `express` template and `mysql` database:

```bash
$ cubejs create demo-app -t express -d mysql
```

Create app called `demo-app` using `serverless` template and `athena` database:

```bash
$ cubejs create demo-app -d athena -t serverless
```

## server

[[warning | Note]]
| To define configuration you should use `cube.js` configuration file. See [available options](https://cube.dev/docs/@cubejs-backend-server-core#options-reference).

The `server` command starts Cube.js in production mode.

Default start:

```bash
$ cubejs server
```

With debug information:

```sh
$ cubejs server --debug
```

### Usage

```bash
$ cubejs server
```

## generate

The `generate` command helps to build data schema for existing database tables.
You can only run `generate` from the Cube.js app directory.
This command could not be used without an active [Database connection](/connecting-to-the-database).

### Usage

```bash
$ cubejs generate -t TABLE-NAMES
```

### Flags

| Parameter | Description | Values |
| --------- | ----------- | ------ |
| -t, --tables <tables> | Comma delimited list of tables to generate schema for. | `TABLE-NAME-1,TABLE-NAME-2` |

### Example

Generate schema files for tables `orders` and `customers`:

```bash
$ cubejs generate -t orders,customers
```

## token

The `token` command generates a JWT Cube.js token. It either uses the value of the `CUBEJS_API_SECRET` environment variable or provided value with `-s` flag.
You can only run `token` command from the Cube.js app directory.

_Use these manually generated tokens in production with caution._ <br> _Please refer to the [Security Guide](https://cube.dev/docs/security) for production security best practices._

### Usage

```bash
$ cubejs token -e TOKEN-EXPIRY -s SECRET -p FOO=BAR -u BAZ=QUX
```

### Flags

| Parameter | Description | Example |
| --------- | ----------- | ------ |
| -e, --expiry &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;  | Token expiry. Set to 0 for no expiry (default: "30 days") | `1 day`, `30 days` &nbsp; &nbsp; &nbsp; &nbsp;  |
| -s, --secret | Cube.js app secret. Also can be set via environment variable `CUBEJS_API_SECRET` | - |
| -p, --payload | Token Payload | `foo=bar`, `userId=2` |
| -u, --user-context | Token USER_CONTEXT Payload | `baz=qux`, `companyId=5` |

### Example

Generate token with 1 day expiry and payload `{ 'appId': 1, 'userId': 2 }`:

```bash
$ cubejs token -e "1 day" -p appId=1 -p userId=2
```
