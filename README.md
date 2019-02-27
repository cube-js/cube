
<p align="center"><a href="https://www.statsbot.co/cubejs"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://statsbot.co/cubejs) • [Blog](https://statsbot.co/blog) • [Slack](https://publicslack.com/slacks/cubejs/invites/new) • [Twitter](https://twitter.com/thecubejs)

[![npm version](https://badge.fury.io/js/%40cubejs-client%2Fcore.svg)](https://badge.fury.io/js/%40cubejs-client%2Fcore)

__Cube.js is an open source modular framework to build analytical web applications__. It is primarily used to build internal business intelligence tools or to add customer-facing analytics to an existing application.

Cube.js was designed to work with Serverless Query Engines like AWS Athena and Google BigQuery. Multi-stage querying approach makes it suitable for handling trillions of data points. Most modern RDBMS work with Cube.js as well and can be tuned for adequate performance.

Unlike others, it is not a monolith application, but a set of modules, which does one thing well. Cube.js provides modules to run transformations and modeling in data warehouse, querying and caching, managing API gateway and building UI on top of that.

### Cube.js Backend

- __Cube.js Schema.__ It acts as an ORM for analytics and allows to model everything from simple counts to cohort retention and funnel analysis.
- __Cube.js Query Orchestration and Cache.__ It optimizes query execution by breaking queries into small, fast, reusable and materialzed pieces.
- __Cube.js API Gateway.__ It provides idempotent long polling API which guarantees analytic query results delivery without request time frame limitations and tolerant to connectivity issues.

### Cube.js Frontend

- __Cube.js Javascript Client.__ It provides idempotent long polling API which guarantees analytic query results delivery without request time frame limitations and tolerant to connectivity issues.
- __Cube.js React.__ React wrapper for Cube.js API.

## Why Cube.js?

If you are building your own business intelligence tool or customer-facing analytics most probably you'll face following problems:

1. __Performance.__ Most of effort time in modern analytics software development is spent to provide adequate time to insight. In the world where every company data is a big data writing just SQL query to get insight isn't enough anymore.
2. __SQL code organization.__ Modelling even a dozen of metrics with a dozen of dimensions using pure SQL queries sooner or later becomes a maintenance nightmare which ends up in building modelling framework.
3. __Infrastructure.__ Key components every production-ready analytics solution requires: analytic SQL generation, query results caching and execution orchestration, data pre-aggregation, security, API for query results fetch, and visualization.

Cube.js has necessary infrastructure for every analytic application that heavily relies on its caching and pre-aggregation layer to provide several minutes raw data to insight delay and sub second API response times on a trillion of data points scale.

## Contents

- [Getting Started](#getting-started)
- [Examples](#examples)
- [Tutorials](#tutorials)
- [Community](#community)
- [Architecture](#architecture)
- [Security](#security)
- [API](#api)
- [License](#license)

## Getting Started

### 1. Install with NPM or Yarn
```bash
$ npm install -g cubejs-cli
# or
$ yarn global add cubejs-cli
```

### 2. Connect to Your Database
Run the following command to get started with Cube.js

```bash
$ cubejs create <project name> -d <database type>
```

specifying the project name and your database using `-d` flag. Available options: 

* `postgres`
* `mysql` 
* `athena`

For example,

```bash
$ cubejs create hello-world -d postgres
```

Once run, the `create` command will create a new project directory that contains the scaffolding for your new Cube.js project. This includes all the files necessary to spin up the Cube.js backend, example frontend code for displaying the results of Cube.js queries in a React app, and some example schema files to highlight the format of the Cube.js Data Schema layer.

The `.env` file in this project directory contains placeholders for the relevant database credentials. For MySQL and PostgreSQL, you'll need to fill in the target host, database name, user and password. For Athena, you'll need to specify the AWS access and secret keys with the [access necessary to run Athena queries](https://docs.aws.amazon.com/athena/latest/ug/access.html), and the target AWS region and [S3 output location](https://docs.aws.amazon.com/athena/latest/ug/querying.html) where query results are stored.

### 3. Define Your Data Schema

Cube.js uses Data Schema to generate and execute SQL.

It acts as an ORM for your database and it is flexible enough to model everything from simple counts to cohort retention and funnel analysis. [Read more about Cube.js Schema](https://statsbot.co/docs/getting-started-cubejs).

You can generate schema files from your database tables using the `cubejs` CLI, or write them manually:

#### Generating Data Schema files for MySQL, Postgres

Since you've defined the target database in the `CUBEJS_DB_NAME` environment variable, in the `.env` file above, you can simply specify a comma-separated list of tables for which you want to generate Data Schema files as the argument for the `-t` option:

```bash
$ cubejs generate -t orders,customers
```

#### Generating Data Schema files for Athena

Generating Data Schema files for Athena requires you to pass the target database and table in the format `db.table`. For example:

```bash
$ cubejs generate -t my_db.orders
```

#### Manually creating Data Schema files

You can also add schema files to the `schema` folder manually:

```javascript
// schema/users.js

cube(`Users`, {
   measures: {
     type: `count`
   },

   dimensions: {
     age: {
       type: `number`,
       sql: `age`
     },

     createdAt: {
       type: `time`,
       sql: `createdAt`
     },

     country: {
       type: `string`,
       sql: `country`
     }
   }
});
```

### 4. Visualize Results
The Cube.js client connects to the Cube.js Backend and lets you visualize your data. This section shows how to use Cube.js Javascript client.

The Cube.js backend requires a Redis server running on your local machine on the default port of `6379`. This default location can be changed by setting the `REDIS_URL` environment variable to your Redis server. Please make sure your Redis server is up before proceeding:

```bash
$ redis-cli ping
PONG
```

As a shortcut you can run your dev server first:

```
$ npm run dev
```

Then open `http://localhost:4000` to see visualization examples. This will open a [codesandbox.io](https://codesandbox.io) sample React app you can edit. You can change the metrics and dimensions of the example to use the schema you defined above, change the chart types, and more!

#### Cube.js Client Installation

Vanilla JS:
```bash
$ npm i --save @cubejs-client/core
```

React:

```bash
$ npm i --save @cubejs-client/core
$ npm i --save @cubejs-client/react
```

#### Example Usage

##### Vanilla Javascript
Instantiate Cube.js API and then use it to fetch data:

```js
import cubejs from '@cubejs-client/core';
import Chart from 'chart.js';
import chartjsConfig from './toChartjsData';

const cubejsApi = cubejs('eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpIjozODU5NH0.5wEbQo-VG2DEjR2nBpRpoJeIcE_oJqnrm78yUo9lasw');

const resultSet = await cubejsApi.load({
  measures: ['Stories.count'],
  timeDimensions: [{
    dimension: 'Stories.time',
    dateRange: ['2015-01-01', '2015-12-31'],
    granularity: 'month'
  }]
})
const context = document.getElementById("myChart");
new Chart(context, chartjsConfig(resultSet));
```

##### React
Import `cubejs` and `QueryRenderer` components, and use them to fetch the data.
In the example below we use Recharts to visualize data.

```jsx
import React from 'react';
import { LineChart, Line, XAxis, YAxis } from 'recharts';
import cubejs from '@cubejs-client/core';
import { QueryRenderer } from '@cubejs-client/react';

const cubejsApi = cubejs('eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpIjozODU5NH0.5wEbQo-VG2DEjR2nBpRpoJeIcE_oJqnrm78yUo9lasw');

export default () => {
  return (
    <QueryRenderer
      query={{
        measures: ['Stories.count'],
        dimensions: ['Stories.time.month']
      }}
      cubejsApi={cubejsApi}
      render={({ resultSet }) => {
        if (!resultSet) {
          return 'Loading...';
        }

        return (
          <LineChart data={resultSet.rawData()}>
            <XAxis dataKey="Stories.time"/>
            <YAxis/>
            <Line type="monotone" dataKey="Stories.count" stroke="#8884d8"/>
          </LineChart>
        );
      }}
    />
  )
}
```


## Examples

| Demo | Code | Description |
|:------|:----------:|:-------------|
|[Examples Gallery](https://statsbotco.github.io/cubejs-client/)|[examples-gallery](https://github.com/statsbotco/cubejs-client/tree/master/examples/examples-gallery)|Examples Gallery with different visualizations libraries|
|[Stripe Dashboard](http://cubejs-stripe-dashboard-example.s3-website-us-west-2.amazonaws.com/)|[stripe-dashboard](https://github.com/statsbotco/cubejs-client/tree/master/examples/stripe-dashboard)|Stripe Demo Dashboard built with Cube.js and Recharts|
|[AWS Web Analytics](https://statsbotco.github.io/cubejs-client/aws-web-analytics/)|[aws-web-analytics](https://github.com/statsbotco/cubejs-client/tree/master/examples/aws-web-analytics)|Web Analytics with AWS Lambda, Athena, Kinesis and Cube.js|
|[Event Analytics](https://d1ygcqhosay4lt.cloudfront.net/)|[event-analytics](https://github.com/statsbotco/cube.js/tree/master/examples/event-analytics)|Mixpanel like Event Analytics App built with Cube.js and Snowplow|

## Tutorials
- [Building a Serverless Stripe Analytics Dashboard](https://statsbot.co/blog/building-serverless-stripe-analytics-dashboard/)
- [Building E-commerce Analytics React Dashboard with Cube.js and Flatlogic](https://statsbot.co/blog/building-analytics-react-dashboard-with-cube.js)
- [Building Open Source Google Analytics from Scratch](https://statsbot.co/blog/building-open-source-google-analytics-from-scratch/)
- [Building MongoDB Dashboard using Node.js](https://statsbot.co/blog/building-mongodb-dashboard-using-node.js)

## Community

If you have any questions or need help - [please join our Slack community](https://publicslack.com/slacks/cubejs/invites/new) of amazing developers and contributors.

## Architecture
__Cube.js acts as an analytics backend__, translating business logic (metrics and dimensions) into SQL and handling database connection. 

The Cube.js javascript Client performs queries, expressed via dimensions, measures, and filters. The Server uses Cube.js Schema to generate a SQL code, which is executed by your database. The Server handles all the database connection, as well as pre-aggregations and caching layers. The result then sent back to the Client. The Client itself is visualization agnostic and works well with any chart library.

<p align="center"><img src="https://i.imgur.com/FluGFqo.png" alt="Cube.js" width="100%"></p>

## Security

Cube.js auth tokens used to access an API are in fact [JWT tokens](https://jwt.io/).
You should use API Secret to generate your own client side auth tokens.
API Secret is generated on app creation and saved in `.env` file as `CUBEJS_API_SECRET` variable.

You can generate two types of tokens:
- Without security context. It implies same data access permissions for all users.
- With security context. User or role-based security models can be implemented using this approach.

Security context can be provided by passing `u` param for payload.
For example if you want to pass user id in security context you can create token with payload:
```json
{
  "u": { "id": 42 }
}
```

In this case `{ id: 42 }` object will be accessible as `USER_CONTEXT` in cube.js Data Schema.
Learn more: [Data Schema docs](https://statsbot.co/docs/cube#context-variables-user-context).

> *NOTE*: We strongly encourage you to use `exp` expiration claim to limit life time of your public tokens.
> Learn more: [JWT docs](https://github.com/auth0/node-jsonwebtoken#token-expiration-exp-claim).

## API

### Cube.js Backend

#### CubejsServerCore.create(options)

Create an instance of `CubejsServerCore` to embed it in an `Express` application.

* `options` - options object.
    * `dbType` - Type of your database.
    * `driverFactory()` - pass function of the driver factory with your database type.
    * `logger(msg, params)` - pass function for your custom logger.
    * `schemaPath` - Path to the `schema` location. By default, it is `/schema`.
    * `devServer` - Enable development server. By default, it is `true`.
    
```javascript
import * as CubejsServerCore from "@cubejs-backend/server-core";
import * as express from 'express';
import * as path from 'path';

const express = express();

const dbType = 'mysql';
const config = {
  dbType,
  devServer: false,
  driverFactory: () => CubejsServerCore.createDriver(dbType),
  logger: (msg, params) => {
    console.log(`${msg}: ${JSON.stringify(params)}`);
  },
  schemaPath: path.join('assets', 'schema')
};

const core = CubejsServerCore.create(config);
await core.initApp(express);
```

### Cube.js Frontend

#### cubejs(apiKey, options)

Create instance of `CubejsApi`.

* `apiKey` - API key used to authorize requests and determine SQL database you're accessing. In the development mode, Cube.js Backend will print the API key to the console on on startup.
* `options` - options object.
   * `apiUrl` - URL of your Cube.js Backend. By default, in the development environment it is http://localhost:4000/cubejs-api/v1.

```javascript
import cubejs from "@cubejs-client/core";

const cubejsApi = cubejs(
  "CUBEJS-API-TOKEN",
  { apiUrl: "http://localhost:4000/cubejs-api/v1" }
);
```

#### CubejsApi.load(query, options, callback)

Fetch data for passed `query`. Returns promise for `ResultSet` if `callback` isn't passed.

* `query` - analytic query. Learn more about it's format below.
* `options` - options object. Can be omitted.
    * `progressCallback(ProgressResult)` - pass function to receive real time query execution progress.
* `callback(err, ResultSet)` - result callback. If not passed `load()` will return promise.

#### QueryRenderer

`<QueryRenderer />` React component takes a query, fetches the given query, and uses the render prop to render the resulting data.

Properties:

- `query`: analytic query. Learn more about it's format below.
- `cubejsApi`: `CubejsApi` instance to use.
- `render({ resultSet, error, loadingState })`: output of this function will be rendered by `QueryRenderer`.
  - `resultSet`: A `resultSet` is an object containing data obtained from the query.  If this object is not defined, it means that the data is still being fetched. `ResultSet` object provides a convient interface for data munipulation.
  - `error`: Error will be defined if an error has occurred while fetching the query.
  - `loadingState`: Provides information about the state of the query loading.
  

#### ResultSet
##### ResultSet.chartPivot()

Returns normalized query result data in the following format.

```js
// For query
{
  measures: ['Stories.count'],
  timeDimensions: [{
    dimension: 'Stories.time',
    dateRange: ['2015-01-01', '2015-12-31'],
    granularity: 'month'
  }]
}

// ResultSet.chartPivot() will return
[
    { "x":"2015-01-01T00:00:00", "Stories.count": 27120 },
    { "x":"2015-02-01T00:00:00", "Stories.count": 25861 },
    { "x": "2015-03-01T00:00:00", "Stories.count": 29661 },
    //...
]
```
##### ResultSet.seriesNames()

Returns the array of series objects, containing `key` and `title` parameters.

```js
// For query
{
  measures: ['Stories.count'],
  timeDimensions: [{
    dimension: 'Stories.time',
    dateRange: ['2015-01-01', '2015-12-31'],
    granularity: 'month'
  }]
}

// ResultSet.seriesNames() will return
[
   { "key":"Stories.count", "title": "Stories Count" }
]
```

#### Query Format

Query is plain JavaScript object, describing an analytics query. The basic elements of query (query members) are `measures`, `dimensions`, and `segments`. You can [learn more about Cube.js Data Schema here.](https://statsbot.co/docs/getting-started-cubejs)
The query member format name is `CUBE_NAME.MEMBER_NAME`, for example dimension email in the Cube Users would have the following name `Users.email`.

Query has following properties:

- `measures`: An array of measures.
- `dimensions`: An array of dimensions.
- `filters`: An array of filters.
- `timeDimensions`: A convient way to specify a time dimension with a filter. It is an array of objects with following keys
  - `dimension`: Time dimension name.
  - `dateRange`: An array of dates with following format '2015-01-01', if only one date specified the filter would be set exactly to this date. 
  - `granularity`: A granularity for a time dimension, supports following values `day|week|month|year`.
- `segments`: An array of segments. Segment is a named filter, created in the Data Schema.
- `limit`: A row limit for your query. The hard limit is set to 5000 rows by default.

```js
{
  measures: ['Stories.count'],
  dimensions: ['Stories.category'],
  filters: [{
    dimension: 'Stories.dead',
    operator: 'equals',
    values: ['No']
  }],
  timeDimensions: [{
    dimension: 'Stories.time',
    dateRange: ['2015-01-01', '2015-12-31'],
    granularity: 'month'
  }],
  limit: 100
}
```

## License

Cube.js Client is [MIT licensed](./packages/cubejs-client-core/LICENSE).

Cube.js Backend is [Apache 2.0 licensed](./packages/cubejs-server/LICENSE).
