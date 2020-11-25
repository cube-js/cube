---
order: 3
title: "Cube.js API with BigQuery"
---

Google BigQuery is a serverless and highly scalable data warehouse. It is
designed to quickly process complex queries on large datasets. It uses
SQL as a query language, which makes it easy to get started.

There are a few things worth mentioning before we proceed. BigQuery isn't designed
for transactional queries like CRUD operations. It takes around 2 seconds to run a simple query like `SELECT * FROM bigquery-public-data.object LIMIT 10` on a 100 KB table with 500 rows. Also, BigQuery is __slower__ on small datasets than traditional relational databases, such as MySQL or Postgres.

BigQuery is a paid service, where customers are charged based on [query and storage costs](https://cloud.google.com/bigquery/pricing). Real-time data streaming is a paid feature as well; you can check [its pricing here](https://cloud.google.com/bigquery/streaming-data-into-bigquery). There are best practices on how to control the amount of processed data per query in order to reduce the cost. We'll talk about them later in this part.

## Prerequisites

You are going to need a Google Cloud Platform (GCP) account in order to use BigQuery. If you don't have it yet, please refer to [this guide](https://cloud.google.com/gcp/getting-started/) to set it up and then come back here to continue our tutorial.

Once you have a GCP project with billing enabled (by starting a free trial or using a coupon, for example), you can move on to the next steps.

As a dataset, we'll use a sample public events dataset—`cubejs-examples.stats.events`. Feel free to use your own dataset if you have one.

## Creating a Cube.js Application

We are going to use Cube.js CLI to create our new Cube.js application with the BigQuery driver:

```bash
$ npx cubejs-cli create real-time-dashboard -d bigquery
```

Now, we need to configure credentials to access BigQuery. Cube.js uses
environment variables to manage database credentials. To connect to BigQuery, we need to set two variables: `CUBEJS_DB_BQ_PROJECT_ID` and `CUBEJS_DB_BQ_KEY_FILE`.

The first one is simply your project ID, which you can copy from the lift of
your projects. The `CUBEJS_DB_BQ_KEY_FILE` variable should point to the [Service
Account Key
File](https://cloud.google.com/bigquery/docs/authentication/service-account-file). To get this file, you need to create a new service account on IAM -> Service accounts page. Add **BigQuery Data Viewer** and **BigQuery Job User** roles to this service account and then generate a new key file. Download it and place it into the `real-time-dashboard` folder.

Your `real-time-dashboard/.env` file should look like the following.

```bash
CUBEJS_DB_BQ_PROJECT_ID=cubejs-examples
CUBEJS_DB_BQ_KEY_FILE=./cubejs-examples-f1c5cbc00a18.json
CUBEJS_DB_TYPE=bigquery
CUBEJS_API_SECRET=SECRET
```

## Data Schema

Cube.js uses the data schema to generate an SQL code, which will be executed in your database. Data schema is a JavaScript code, which defines measures and dimensions and how they map to SQL queries. You can learn more about data schema and its features [here](https://cube.dev/docs/getting-started-cubejs-schema).

As mentioned before, we are going to use data from a public BigQuery table—`cubejs-examples.stats.events`. Inside the project folder, create the `schema/Events.js` file with the following content.

```javascript
cube(`Events`, {
  sql: `
    SELECT
      *
    FROM
      stats.events
    WHERE ${FILTER_PARAMS.Events.timestamp.filter('timestamp')}`,

  refreshKey: {
    sql: `
      SELECT
        count(*)
      FROM
        stats.events
      WHERE ${FILTER_PARAMS.Events.timestamp.filter('timestamp')}`
  },

  measures: {
    count: {
      type: `count`
    },

    online: {
      type: `countDistinct`,
      sql : `${anonymousId}`,
      filters: [
        { sql: `${timestamp} > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)` }
      ]
    },

    pageView: {
      type: `count`,
      filters: [
        { sql: `${eventType} = 'pageView'` }
      ]
    },

    buttonClick: {
      type: `count`,
      filters: [
        { sql: `${eventType} = 'buttonCLicked'` }
      ]
    }
  },

  dimensions: {
    anonymousId: {
      sql: `anonymousId`,
      type: `string`
    },

    eventType: {
      sql: `eventType`,
      type: `string`
    },

    timestamp: {
      sql: `timestamp`,
      type: `time`
    }
  }
});
```

The `sql` property of the cube defines the SQL that will be used to generate a
table that will be queried by a cube. It usually takes the form of a `SELECT *
FROM table` query. In our case, you can see we are using
[FILTER_PARAMS](https://cube.dev/docs/cube#context-variables-filter-params)
here. Usually you don't need to pass filters to the `sql` property and filtering
is done automatically by Cube.js, but in the case of [BigQuery partitioned
tables](https://cloud.google.com/bigquery/docs/partitioned-tables), you need to
do that. The `events` table is partitioned by a timestamp and cannot be queried
without a filter over the `timestamp` column. BigQuery partitioned tables is an excellent way to reduce the cost and improve the performance of our queries.

Next, we define measures for our dashboard. The `count` measure is just a simple count
of all total events; `pageView` and `buttonClick` are counts of corresponding
events. The `online` measure is a bit more complex. It returns the number of unique
users who performed any event in the last 3 minutes.

Within `dimensions` we have the simple `anonymousId`, `eventType`, and `timestamp`,
which just shows the values from corresponding columns. We've also defined a
`secondsAgo` dimension, which calculates the number of seconds since the event's
occurrence.

Lastly, we are setting a custom
[refreshKey](https://cube.dev/docs/cube#parameters-refresh-key). It controls
the refresh of the Cube.js in-memory cache layer. We're making it to count the
number of rows in our table. This way Cube.js will not issue unnecessary queries
against BigQuery, which would help to keep our billing low.

Feel free to play around with measures and dimensions in the playground. Please
make sure you always select some date range, since it is required because of the
partitioning.

![](/images/2-screenshot-3.png)

We will use these measures and dimensions in the next part, when we create a frontend dashboard app with React and Chart.js.
