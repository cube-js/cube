---
title: Command Reference
permalink: /reference
category: Cube.js CLI
menuOrder: 2
---

## create

`create` command generates barebones Cube.js app.

### Usage

```bash
$ cubejs create APP-NAME -d DB-TYPE [-t TEMPLATE]
```

### Flags

| Parameter | Description | Values |
| --------- | ----------- | ------ |
| -d, --db-type <db-type> | Preconfigure Cube.js app for selected database. | `postgres`, `mysql`, `athena`, `mongobi`, `bigquery` |
| -t, --template <template> | Framework running Cube.js backend. | `express` (default), `serverless` |

### Example

Create app called `demo-app` using default (`express`) template and `mysql` database:

```bash
$ cubejs create demo-app -d mysql
```

Create app called `demo-app` using `serverless` template and `athena` database:

```bash
$ cubejs create demo-app -d athena -t serverless
```

## generate

`generate` command helps building data schema for existing database tables.
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
