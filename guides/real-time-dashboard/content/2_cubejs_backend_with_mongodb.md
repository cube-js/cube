---
order: 2
title: "Cube.js API with MongoDB"
---

_Feel free to jump to the [next part](cube-js-backend-with-big-query) if you want to use BigQuery instead of
MongoDB_

For quite a long time, doing analytics with MongoDB required additional overhead
compared to modern SQL RDBMS and Data Warehouses associated with aggregation pipeline and MapReduce practices. To fill this gap, MongoDB released the MongoDB connector for BI, which acts as a MySQL server on top of your MongoDB data. Under the hood, it bridges existing aggregation mechanisms to the MySQL protocol, allowing standard MySQL clients to connect and issue SQL queries.

## Setting up MongoDB and BI Connector

If you don’t have a MongoDB instance, you can download it [here](https://www.mongodb.com/download-center/community). The BI Connector can be downloaded [here](https://www.mongodb.com/download-center/bi-connector). Please make sure you use the MongoDB version that supports the MongoDB connector for BI. 

After the BI connector has been installed, please start a `mongod` instance first. If you use the downloaded installation, it can be started from its home directory like so:

```bash
$ bin/mongod
```

The BI connector itself can be started the same way:

```bash
$ bin/mongosqld
```

Please note that `mongosqld` resides in another `bin` directory. If everything works correctly, you should see a success log message in your shell for the `mongosqld` process:

```bash
[initandlisten] waiting for connections at 127.0.0.1:3307
```

If you’re using the MongoDB Atlas, you can use [this guide](https://docs.atlas.mongodb.com/bi-connection/#bi-connection) to enable BI connector.

## Getting a Sample Dataset

_You can skip this step if you already have data for your dashboard._

We host a sample events collection, which you can use for a demo dashboard. Use
the following commands to download and import it.

```bash
$ curl https://cube.dev/downloads/events-dump.zip > events-dump.zip
$ unzip events-dump.zip
$ bin/mongorestore dump/stats/events.bson
```

Please make sure to restart the MongoDB BI connector instance in order to generate an up-to-date MySQL schema from the just added collection.

## Creating Cube.js Application

We are going to use Cube.js CLI to create our new Cube.js application with the MongoBI driver:

```bash
$ npx cubejs-cli create real-time-dashboard -d mongobi
```

Go to the just created `real-time-dashboard` folder and update the `.env` file with your
MongoDB credentials.

```env
CUBEJS_DB_HOST=localhost
CUBEJS_DB_NAME=stats
CUBEJS_DB_PORT=3307
CUBEJS_DB_TYPE=mongobi
CUBEJS_API_SECRET=SECRET
```

Now let's start a Cube.js development server.

```bash
$ npm run dev
```

This starts a development server with a playground. We'll use it to generate Cube.js schema, test our data and, finally, build a dashboard. Open [http://localhost:4000](http://localhost:4000) in your browser.

Cube.js uses the data schema to generate an SQL code, which will be executed in your database. 
Data schema is a JavaScript code, which defines measures and dimensions and how they map to SQL queries.

Cube.js can generate a simple data schema based on the database’s tables. Select the `events` table and click “Generate Schema.”

![](/images/2-screenshot.png)

Once the schema is generated, we can navigate to the “Build” tab and select some measures and dimensions to test out the schema. The "Build" tab is a place where you can build sample charts with different visualization libraries and inspect how that chart was created, starting from the generated SQL all the way up to the JavaScript code to render the chart. You can also inspect the JSON query, which is sent to the Cube.js backend.

![](/images/2-screenshot-2.png)

Although auto-generated schema is a good way to get started, in many cases you'd need to
add more complex logic into your Cube.js schema. You can learn more about data
schema and its features
[here](https://cube.dev/docs/getting-started-cubejs-schema). In our case, we
want to create several advanced measures and dimensions for our real-time dashboard.

Replace the content of `schema/Events.js` with the following.

```javascript
cube(`Events`, {
  sql: `SELECT * FROM stats.events`,

  refreshKey: {
    sql: `SELECT UNIX_TIMESTAMP()`
  },

  measures: {
    count: {
      type: `count`
    },

    online: {
      type: `countDistinct`,
      sql : `${anonymousId}`,
      filters: [
        { sql: `${timestamp} > date_sub(now(), interval 3 minute)` }
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
    secondsAgo: {
      sql: `TIMESTAMPDIFF(SECOND, timestamp, NOW())`,
      type: `number`
    },

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

First, we define measures for our dashboard. The `count` measure is just a simple count
of all total events; `pageView` and `buttonClick` are counts of corresponding
events. The `online` measure is a bit more complex. It returns the number of unique
users who performed any event in the last 3 minutes.

Within `dimensions` we have simple `anonymousId`, `eventType`, and `timestamp`,
which just shows the values from corresponding columns. We've also defined a
`secondsAgo` dimension, which calculates the number of seconds since the event's
occurrence.

Lastly, we are setting a custom
[refreshKey](https://cube.dev/docs/cube#parameters-refresh-key). It controls
the refresh of the Cube.js in-memory cache layer. Setting it to `SELECT
UNIX_TIMESTAMP()` will refresh the cache every second. You need to carefully
select the best refresh strategy depending on your data to get the freshest
data when you need it, but, at the same time, not overwhelm the database with a lot of unnecessary queries.

We will use these measures and dimensions in the next part, when we create a frontend dashboard app with React and Chart.js.
