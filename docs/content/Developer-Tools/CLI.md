---
title: CLI
permalink: /using-the-cubejs-cli
category: Developer Tools
menuOrder: 1
---

The Cube.js command line interface (CLI) is used for various Cube.js workflows.
It could help you in areas such as:

- Creating a new Cube.js service;
- Generating a schema based on your database tables;

## Quickstart

Once installed, run the following command to create new Cube.js service

```bash
$ npx cubejs-cli create <project name> -d <database type>
```

specifying the project name and your database using `-d` flag. Available
options:

- `postgres`
- `mysql`
- `mongobi`
- `athena`
- `redshift`
- `bigquery`
- `mssql`
- `clickhouse`
- `snowflake`
- `presto`

For example,

```bash
$ npx cubejs-cli create hello-world -d postgres
```

Once run, the `create` command will create a new project directory that contains
the scaffolding for your new Cube.js project. This includes all the files
necessary to spin up the Cube.js backend, example frontend code for displaying
the results of Cube.js queries in a React app, and some example schema files to
highlight the format of the Cube.js Data Schema layer.

The `.env` file in this project directory contains placeholders for the relevant
database credentials. For MySQL, Redshift, and PostgreSQL, you'll need to fill
in the target host, database name, user and password. For Athena, you'll need to
specify the AWS access and secret keys with the [access necessary to run Athena
queries][link-athena-access], and the target AWS region and [S3 output
location][link-athena-output] where query results are stored.

[link-athena-access]: https://docs.aws.amazon.com/athena/latest/ug/access.html
[link-athena-output]: https://docs.aws.amazon.com/athena/latest/ug/querying.html
