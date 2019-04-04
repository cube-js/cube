---
title: Connecting to the Database
permalink: /connecting-to-the-database
category: Cube.js Backend
menuOrder: 1
---

Cube.js currently provides connectors to the following databases:

| Database             | Cube.js DB Type |
| -------------------- |---------------- |
| PostgreSQL           | postgres      |
| MySQL                | mysql         |
| AWS Athena           | athena        |
| MongoDB (via MongoDB Connector for BI)           | mongobi        |
| Google BigQuery           | bigquery        |


_To use Cube.js with MongoDB you need to install MongoDB Connector for BI. You
can download it [here](https://www.mongodb.com/download-center/bi-connector). [Learn more about setup for MongoDB
here.](https://cube.dev/blog/building-mongodb-dashboard-using-node.js)_

### Configuring Connection for Cube.js CLI Created Apps

When you create a new Cube.js service with the [Cube.js CLI](using-the-cubejs-cli), the `.env` will be
generated to manage all connection credentials. The set of variables could be different based on your database type. For example, for PostgreSQL the `.env` will look like this:


```bash
CUBEJS_DB_HOST=<YOUR_DB_HOST_HERE>
CUBEJS_DB_NAME=<YOUR_DB_NAME_HERE>
CUBEJS_DB_USER=<YOUR_DB_USER_HERE>
CUBEJS_DB_PASS=<YOUR_DB_PASS_HERE>
CUBEJS_DB_TYPE=postgres
CUBEJS_API_SECRET=secret
```

The table below shows which environment variables are used for different databases:

| Database             | Credentials    |
| -------------------- |--------------- |
| PostgreSQL, MySQL, MongoDB    | `CUBEJS_DB_TYPE`, `CUBEJS_DB_HOST`, `CUBEJS_DB_NAME`, `CUBEJS_DB_USER`, `CUBEJS_DB_PASS` |
| AWS Athena           | `CUBEJS_DB_TYPE`, `CUBEJS_AWS_KEY`, `CUBEJS_AWS_SECRET`, `CUBEJS_AWS_REGION`, `CUBEJS_AWS_S3_OUTPUT_LOCATION` |
| Google Bigquery           | `CUBEJS_DB_BQ_PROJECT_ID`, `CUBEJS_DB_BQ_KEY_FILE or CUBEJS_DB_BQ_CREDENTIALS` |

## Notes

### AWS Athena

For Athena, you'll need to specify the AWS access and secret keys with the [access necessary to run Athena queries](https://docs.aws.amazon.com/athena/latest/ug/access.html), and the target AWS region and [S3 output location](https://docs.aws.amazon.com/athena/latest/ug/querying.html) where query results are stored.

### Google BigQuery

In order to connect BigQuery to Cube.js, you need to provide service account credentials.
Cube.js requires the service account to have **BigQuery Data Viewer** and **BigQuery Job User** roles enabled.
You can set `CUBEJS_DB_BQ_KEY_FILE` environment variable with a path to **JSON** key file.

Another way is to encode the key file with **base64**:

```bash
$ cat /path/to/key-file.json | base64
```

Now you can set the `CUBEJS_DB_BQ_CREDENTIALS` environment variable with the base64-encoded key.

### Contributing

If you'd like to connect a database that is not yet supported, you can create a Cube.js-compilant driver package. [Here's a simple step-by-step guide](https://github.com/statsbotco/cube.js/blob/master/CONTRIBUTING.md#implementing-driver).