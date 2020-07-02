---
order: 3
title: "Analytics API with Cube.js"
---

We'll build our analytics API on top of the Athena with [Cube.js](https://github.com/cube-js/cube.js). Cube.js is an open-source framework for building analytical web applications. It creates an analytics API on top of the database and handles things like SQL organization, caching, security, authentication, and much more.

Let's install Cube.js CLI and use it to create our application. Run the following commands in your terminal:

```bash
$ npm install -g cubejs-cli
$ cubejs create react-dashboard -d athena
```

Once run, the `create` command will create a new project directory that contains the scaffolding for your new Cube.js project. Cube.js uses environment variables starting with CUBEJS_ for configuration. To configure the connection to Athena, we need to specify the AWS access and secret keys with the access necessary to run Athena queries, and the target AWS region and S3 output location where query results are stored.

```
CUBEJS_DB_TYPE=athena
CUBEJS_AWS_KEY=<YOUR ATHENA AWS KEY HERE>
CUBEJS_AWS_SECRET=<YOUR ATHENA SECRET KEY HERE>
CUBEJS_AWS_REGION=<AWS REGION STRING, e.g. us-east-1>
# You can find the Athena S3 Output location here: https://docs.aws.amazon.com/athena/latest/ug/querying.html
CUBEJS_AWS_S3_OUTPUT_LOCATION=<S3 OUTPUT LOCATION>
```

Next, let's create a sample data schema for our events. Cube.js uses the data schema to generate SQL code, which will be executed in the database. The data schema is not a replacement for SQL. It is designed to make SQL reusable and give it a structure while preserving all of its power. We can build complex data models with Cube.js data schema. You can learn more about [Cube.js data schema here](https://cube.dev/docs/getting-started-cubejs-schema).

Create a `schema/Events.js` file with the following content.

```javascript
cube(`Events`, {
  sql: `
    SELECT
      event_id,
      event,
      platform,
      derived_tstamp,
      domain_sessionidx,
      domain_sessionid,
      domain_userid,
      ROW_NUMBER() OVER (PARTITION BY domain_sessionid ORDER BY derived_tstamp) AS event_in_session_index
    FROM
       analytics.snowplow_events
  `,

  measures: {
    count: {
      type: `count`,
    },
  },

  dimensions: {
    timestamp: {
      type: `time`,
      sql: `derived_tstamp`
    },

    id: {
      sql: `event_id`,
      type: `string`,
      primaryKey: true
    }
  }
})
```

Please, note that we query `snowplow_events` table from `analytics` database.
Your database and table name may be different

Now, we can start Cube.js server and open [http://localhost:4000](http://localhost:4000). In development mode, Cube.js run Playground. It is an application to help you explore the data schema and send test queries.

Let's test our newly created data schema!
Cube.js accepts queries as JSON objects in the [specific query format](https://cube.dev/docs/query-format). Playground lets you visually build and explore queries. For example, we can construct the test query to load all the events over time like on the screenshot below.

SCREENSHOT

Or we can group queries by X. You can always inspect the underlying JSON query by clicking **JSON Query** button.

GIF

You can explore other queries as well, test different charting libraries used to
visualize results and explore the frontend javascript code. If you are just starting with Cube.js I recommend checking [this tutorial](https://cube.dev/blog/cubejs-open-source-dashboard-framework-ultimate-guide/) as well.

In the next part, we'll start working on the frontend application and will
steadily build out our data schema.
