---
title: Connecting to the Database
permalink: /connecting-to-the-database
category: Cube.js Backend
---

Cube.js currently provides connectors to the following databases:

| Database             | Cube.js DB Type |
| -------------------- |---------------- |
| PostgreSQL           | postgres      |
| MySQL                | mysql         |
| AWS Athena           | athena        |
| MongoDB (via MongoDB Connector for BI)           | mysql        |

MongoDB Connector for BI acts as a MySQL server on top of your MongoDB data, so
you need to set `CUBEJS_DB_TYPE` to `mysql`. [Learn more about setup for MongoDB
here.](https://statsbot.co/blog/building-mongodb-dashboard-using-node.js)

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

For Athena, you'll need to specify the AWS access and secret keys with the [access necessary to run Athena queries](https://docs.aws.amazon.com/athena/latest/ug/access.html), and the target AWS region and [S3 output location](https://docs.aws.amazon.com/athena/latest/ug/querying.html) where query results are stored.
